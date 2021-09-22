package mx.j2.recommend.data_source;

import com.alicp.jetcache.Cache;
import com.alicp.jetcache.MultiLevelCacheBuilder;
import com.alicp.jetcache.RefreshPolicy;
import com.alicp.jetcache.embedded.CaffeineCacheBuilder;
import com.alicp.jetcache.redis.lettuce.RedisLettuceCacheBuilder;
import com.alicp.jetcache.support.FastjsonKeyConvertor;
import com.alicp.jetcache.support.JavaValueDecoder;
import com.alicp.jetcache.support.JavaValueEncoder;
import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import mx.j2.recommend.conf.Conf;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.pool_conf.PoolConf;
import mx.j2.recommend.pool_conf.PoolConfParser;
import mx.j2.recommend.util.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author ：zhongrenli
 * @date ：Created in 5:06 下午 2020/8/18
 */
public class PoolConfDataSource extends BaseDataSource {
    private static final Logger logger = LogManager.getLogger(PoolConfDataSource.class);
    private static final String CACHE_KEY = "pool_conf_v2";
    private static final int REFRESH_INTERVAL_IN_SECONDS = 60;
    private volatile Map<String, Map<String, PoolConf>> allLevelPoolConfMap;

    /**
     * 池子配置内容字符串缓存
     * 多级缓存，local - redis
     */
    private Cache<String, String> poolConfContent;

    public PoolConfDataSource() {
        //init();延迟到 DataSourceManager 初始化完成后
        logger.info("{\"dataSourceInfo\":\"[PoolConfDataSource init successfully]\"}");
    }

    public void init() {
        allLevelPoolConfMap = new LinkedHashMap<>();

//        initConfLocal();
        initConfRemote();
    }

    /**
     * 初始化缓存实例
     */
    private void initCache() {
        RedisURI redisUri = RedisURI.Builder.redis(Conf.getRedisCacheHost(), Conf.getRedisCachePort())
                .withTimeout(Duration.ofMillis(Conf.getJedisClusterSocketTimeout()))
                .build();
        RedisClusterClient redisClient = RedisClusterClient.create(redisUri);
        redisClient.setOptions(ClusterClientOptions.builder()
                .autoReconnect(true)
                .build());

        Cache<String, String> recallLocalCache = CaffeineCacheBuilder.createCaffeineCacheBuilder()
                .limit(1)
                .buildCache();

        Cache<String, String> recallRemoteCache = RedisLettuceCacheBuilder.createRedisLettuceCacheBuilder()
                .keyConvertor(FastjsonKeyConvertor.INSTANCE)
                .valueDecoder(JavaValueDecoder.INSTANCE)
                .valueEncoder(JavaValueEncoder.INSTANCE)
                .redisClient(redisClient)
                .keyPrefix("pool_conf")// 预发布测试的时候改一下，为 Recall_
                .buildCache();

        RefreshPolicy policy = RefreshPolicy.newPolicy(REFRESH_INTERVAL_IN_SECONDS, TimeUnit.SECONDS);
        poolConfContent = MultiLevelCacheBuilder.createMultiLevelCacheBuilder()
                .addCache(recallLocalCache, recallRemoteCache)
                .loader(this::load)
                .refreshPolicy(policy)
                .buildCache();
    }

    /**
     * 远端拉取版本
     */
    private void initConfRemote() {
        //initCache();// 缓存先下掉，可能不用了
        initConf();
        startRefreshScheduledTask();
    }

    /**
     * 本地解析版本
     */
    private void initConfLocal() {
        parseConfLocal();
    }

    /**
     * 初始化配置
     */
    private void initConf() {
        /*
         * 尝试 3 次
         */
        int retry = -3;

        while (retry < 0) {
            if (parseConfRemote()) {
                break;// 成功退出
            } else {//
                retry++;
            }
        }

        // 3 次都失败了，杀死进程
        if (retry == 0) {
            LogTool.reportError(DefineTool.ErrorEnum.FATAL, logger, new Exception("Parse remote pool config failed."));
        }
    }

    /**
     * 解析本地配置文件
     * 目前 pool.json 只归档用，不解析使用
     */
    private boolean parseConfLocal() {
        //本地文件
        String confContent = FileTool.readContent(Conf.getPoolConf());

        if (MXStringUtils.isNotEmpty(confContent)) {
            return parseConfContent(confContent);
        } else {
            logger.error("Failed to get remote pool config.");
            return false;
        }
    }

    /**
     * 解析远端（REDIS）配置
     */
    private boolean parseConfRemote() {
        // 拿远端的配置信息
        String confContent = getConfContentFromCA();

        if (MXStringUtils.isNotEmpty(confContent)) {
            return parseConfContent(confContent);
        } else {
            logger.error("Failed to get remote pool config.");
            return false;
        }
    }

    /**
     * 解析配置内容（JSON 字符串格式）
     */
    private boolean parseConfContent(String confContent) {
        Map<String, Map<String, PoolConf>> allLevelMap = new LinkedHashMap<>();

        try {
            PoolConfParser.parsePoolConfContent(confContent, allLevelMap);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Parse remote pool config failed: " + confContent + e.getMessage());
            return false;// 回去吧，失败了
        }

        // 要加个判断，安全一点
        if (allLevelMap.isEmpty()) {
            logger.error("Pool config parse error: Empty pools.");
            return false;// 你也回去吧
        }

        sortByDescent(allLevelMap);
        allLevelPoolConfMap = allLevelMap;

        logger.info("Current pool config: " + allLevelMap);

        return true;
    }

    /**
     * 去 REDIS 拿配置字符串
     */
    private String getConfContent() {
//        ElasticCacheSource redisDS = DataSourceManager.INSTANCE.getElasticCacheSource();
//        return redisDS.getString(REDIS_KEY);
        return poolConfContent.get(CACHE_KEY);
    }

    private String getConfContentFromCA() {
        String tableName = DefineTool.Recall.Config.Pools.Table.NAME;
        String env = getConfigEnv();
        String queryFormat = DefineTool.Recall.Config.Pools.query(env);
        String columnName = DefineTool.Recall.Config.Pools.Table.COLUMN_CONTENT;
        return MXDataSource.cassandra().getString(queryFormat, tableName, columnName);
    }

    /**
     * 获取配置环境 dev/pre/prod
     */
    private String getConfigEnv() {
        if (MXJudgeUtils.isDevEnv()) {// 测试
            return DefineTool.Env.DEV.confValue;
        } else if (MXJudgeUtils.isProdEnv()) {// 线上
            return DefineTool.Env.PROD.confValue;
        } else {// 预发布
            return DefineTool.Env.PRE.confValue;
        }
    }

    /**
     * 缓存为空时回调，创建内容
     * 本实例从 CA 中读取并返回
     */
    private String load(String key) {
        String tableName = DefineTool.Recall.Config.Pools.Table.NAME;
        // attention env !!!
        String queryFormat = DefineTool.Env.PRE.query();
        String columnName = DefineTool.Recall.Config.Pools.Table.COLUMN_CONTENT;
        return MXDataSource.cassandra().getString(queryFormat, tableName, columnName);
    }

    /**
     * 开启定时刷新配置任务
     */
    private void startRefreshScheduledTask() {
        ScheduledExecutorService serviceNormal = Executors.newSingleThreadScheduledExecutor();

        int setScheduleSuccesfull = -3;
        while (setScheduleSuccesfull < 0) {
            try {
                serviceNormal.scheduleAtFixedRate(this::parseConfRemote,
                        REFRESH_INTERVAL_IN_SECONDS,
                        REFRESH_INTERVAL_IN_SECONDS,
                        TimeUnit.SECONDS);
                setScheduleSuccesfull = 0;
            } catch (Exception e) {
                setScheduleSuccesfull++;
                e.printStackTrace();
                logger.error("Start refresh pool config schedule task failed. " + e.getMessage());
            }
        }
    }

    private void fillPoolConfMap(Map<String, PoolConf> map, Map<String, PoolConf> fillConfMap) {
        List<Map.Entry<String, PoolConf>> list = new ArrayList<>(map.entrySet());
        list.sort((o1, o2) -> o2.getValue().priority - o1.getValue().priority);
        for (Map.Entry<String, PoolConf> entry : list) {
            fillConfMap.put(entry.getKey(), entry.getValue());
        }
    }

    /**
     * 排序后插入，使迭代时有序（倒序）
     */
    private void sortByDescent(Map<String, Map<String, PoolConf>> map) {
        List<Map.Entry<String, Map<String, PoolConf>>> list = new ArrayList<>(map.entrySet());
        list.sort((o1, o2) -> Integer.parseInt(o2.getKey()) - Integer.parseInt(o1.getKey()));
        map.clear();
        for (Map.Entry<String, Map<String, PoolConf>> entry : list) {
            map.put(entry.getKey(), entry.getValue());
        }
    }

    /**
     * 根据池子名称返回池子配置
     */
    public Map<String, PoolConf> get(String name) {
        if (allLevelPoolConfMap.containsKey(name)) {
            return allLevelPoolConfMap.get(name);
        }
        return null;
    }

    /**
     * 拿所有池子
     */
    public Map<String, Map<String, PoolConf>> all() {
        return allLevelPoolConfMap;
    }

    /**
     * 返回指定级别的index
     */
    public PoolConf getPoolConfByLevel(int level, BaseDataCollection baseDc) {
        Map<String, Map<String, PoolConf>> map = MXDataSource.pools().all();
        if(map == null) {
            return null;
        }
        Map<String, PoolConf> mapOfThisLevel = map.get(String.valueOf(level));
        if(mapOfThisLevel == null) {
            return null;
        }

        PoolConf pc = mapOfThisLevel.getOrDefault(baseDc.recommendFlow.name, mapOfThisLevel.get("base"));
        return pc;
    }
}
