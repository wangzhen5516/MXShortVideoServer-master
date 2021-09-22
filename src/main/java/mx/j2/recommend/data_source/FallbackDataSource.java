package mx.j2.recommend.data_source;

import com.alibaba.fastjson.JSON;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.newrelic.api.agent.Trace;
import mx.j2.recommend.conf.Conf;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_model.document.ShortDocument;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.FileTool;
import mx.j2.recommend.util.MXJudgeUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 服务降级数据源
 * 当服务(接口)不可用时（参触发条件），返回预存的数据作为降级数据
 * <p>
 * 触发条件：
 * 1 timeout
 * 2 rate limit
 * 3 主接口（如 hot）返回空的 Response
 */
public enum FallbackDataSource {
    INSTANCE;

    /**
     * 保底功能开关状态
     */
    private enum Switch {
        OPEN,//  打开，先走推荐，然后有条件的走保底数据
        OPEN_FORCE,// 打开，只走保底，不走推荐
        CLOSE// 关闭，不走保底
    }

    // <editor-fold desc="Internal">

    private final Logger logger;

    /**
     * 保底开关的 redis key
     */
    private static final String REDIS_KEY_FALLBACK_SWITCH = "fallback_switch";

    /**
     * 本地保底数据集合
     */
    private ArrayList<BaseDocument> totalFallback;

    /**
     * 当次返回的保底数量最大值，应 >= request.num
     */
    private static final int MAX_RESPONSE_NUM = 15;

    /**
     * 当前使用的保底数据集合，定时更新
     */
    private List<BaseDocument> randomFallback = new ArrayList<>(MAX_RESPONSE_NUM);

    /**
     * 读写锁
     */
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final Lock readLock = lock.readLock();
    private final Lock writeLock = lock.writeLock();

    /**
     * 保底开关
     */
    private Switch fallbackSwitch = Switch.CLOSE;

    /**
     * 构造函数
     */
    FallbackDataSource() {
        logger = LogManager.getLogger(FallbackDataSource.class);
    }

    /**
     * 初始化
     */
    public void init() {
        // 加载保底数据
        if (!load()) {
            logger.error("Failed to load fallback data");
            return;
        }

        // 调用一遍更新流程作为初始化
        updateFallback();

        // 开启定时更新任务
        startUpdateTask();

        logger.info("{\"dataSourceInfo\":\"[FallbackDataSource init successfully]\"}");
    }

    /**
     * 加载保底数据
     */
    private boolean load() {
        // 加载用于保底的视频 ID 列表
        List<String> idList = loadVideoIds();

        if (MXJudgeUtils.isEmpty(idList)) {
            return false;
        }

        logger.info("Fallback data size: " + idList.size());

        // 封装结果列表作为保底数据集合
        packToResult(idList);

        return true;
    }

    /**
     * 加载保底数据的 id 列表
     */
    private List<String> loadVideoIds() {
        String content = FileTool.readContent(Conf.getFallbackVideosConf());
        return JSON.parseArray(content).toJavaList(String.class);
    }

    /**
     * 开启定时生成任务
     */
    private void startUpdateTask() {
        ScheduledExecutorService executor = new ScheduledThreadPoolExecutor(1,
                new ThreadFactoryBuilder().setNameFormat("recommend-fallback-update-%s").build());

        // 每隔 5 秒钟生成一次要返回的保底数据
        executor.scheduleWithFixedDelay(this::updateFallback, 5, 5, TimeUnit.SECONDS);
    }

    /**
     * 更新保底状态
     */
    private void updateFallback() {
        // 先去更新一把开关状态
        updateFallbackSwitch();

        // 更新保底集合
        if (isOpen()) {
            updateRandomFallback();
        }
    }

    /**
     * 更新当前的保底开关状态
     */
    private void updateFallbackSwitch() {
        // 拿远端的开关状态
        Switch remoteSwitch = getRemoteFallbackSwitch();

        // 跟当前的开关不同
        if (fallbackSwitch != remoteSwitch) {
            fallbackSwitch = remoteSwitch;
            logger.info("Fallback switch to " + fallbackSwitch);
        }

        // 打印当前的开关状态
        logger.info("Current fallback switch: " + fallbackSwitch);
    }

    /**
     * 生成随机保底数据
     */
    private void updateRandomFallback() {
        List<BaseDocument> nextFallback = generateFallbackByRandomN();

        // 加写锁
        writeLock.lock();

        try {
            randomFallback.clear();
            randomFallback.addAll(nextFallback);
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * 拿远端开关状态
     */
    private Switch getRemoteFallbackSwitch() {
        ElasticCacheSource redisDS = MXDataSource.redis();
        String switchString = redisDS.getString(REDIS_KEY_FALLBACK_SWITCH);
        Switch remoteSwitch = fallbackSwitch;

        try {
            // 注意，此处要求拿到的字符串必须和枚举值一样: OPEN, OPEN_FORCE, CLOSE
            remoteSwitch = Switch.valueOf(switchString);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Failed to acquire fallback switch - Wrong value: " + switchString);
        }

        return remoteSwitch;
    }

    /**
     * 随机抽取 N 个元素作为保底数据
     */
    private List<BaseDocument> generateFallbackByRandomN() {
        // 如果保底集合数据小于既定的保底返回数
        if (totalFallback.size() <= MAX_RESPONSE_NUM) {
            return totalFallback;
        }

        // 打乱保底数据集合
        Collections.shuffle(totalFallback);

        // 随机选取一个开始位置
        int start = (int) (System.currentTimeMillis() % (totalFallback.size() - MAX_RESPONSE_NUM));

        // 选择一个区段，包含 MAX_FALLBACK_NUM 个元素
        return totalFallback.subList(start, start + MAX_RESPONSE_NUM);
    }

    /**
     * 打包为结果列表，用于直接返回数据给客户端
     */
    private void packToResult(List<String> idList) {
        // 初始化容器
        totalFallback = new ArrayList<>(idList.size());

        // 打包成 result 列表保存
        for (String id : idList) {
            totalFallback.add(new ShortDocument(id));
        }
    }

    /**
     * 获取当前的保底数据集合
     */
    private List<BaseDocument> getRandomFallback() {
        List<BaseDocument> retList;

        // 加读锁
        readLock.lock();

        // 复制一份当前的保底数据
        try {
            retList = new ArrayList<>(randomFallback);
        } catch (Exception e) {
            retList = Collections.emptyList();
        } finally {
            readLock.unlock();
        }

        return retList;
    }

    /**
     * 保底是打开的
     */
    public boolean isOpen() {
        return fallbackSwitch != Switch.CLOSE;
    }

    /**
     * 强制走保底
     */
    public boolean isForceOpen() {
        return fallbackSwitch == Switch.OPEN_FORCE;
    }

    // </editor-fold>

    // <editor-fold desc="API">

    /**
     * 返回随机的保底数据
     */
    @Trace(dispatcher = true)
    public List<BaseDocument> getRandomFallback(int count) {
        // 当前的随机保底数据集合
        List<BaseDocument> randomFallback = getRandomFallback();

        // 请求数大于等于保底数，直接返回所有保底数据
        if (count >= randomFallback.size()) {
            return randomFallback;
        }

        // 请求数小于保底数，返回头部子集
        return randomFallback.subList(0, count);
    }

    // </editor-fold>
}
