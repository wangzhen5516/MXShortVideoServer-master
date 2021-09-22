package mx.j2.recommend.recall.impl;

import com.newrelic.api.agent.NewRelic;
import mx.j2.recommend.data_model.UserProfile;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_source.LocalCacheDataSource;
import mx.j2.recommend.data_source.database.cassandra.UserProfileStrategyTagCA;
import mx.j2.recommend.manager.DataSourceManager;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.pool_conf.StrategyPoolConf;
import mx.j2.recommend.task.StrategyPoolExecutor;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.LogTool;
import mx.j2.recommend.util.MXJudgeUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * tag 偏好召回器
 */
@SuppressWarnings("unused")
public class UserProfileTagRecall extends BaseRecall<BaseDataCollection> {
    private static final Logger logger = LogManager.getLogger(UserProfileTagRecall.class);
    private static final String KEY_NUM = "num";// 需要的 tag 数量
    private static final String KEY_TABLE = "table";// CA tag 表
    private static final String KEY_POOL = "pool";// 从哪级池子召回
    private static final int MAX_TAG_AMOUNT = 100;
    private static final int MAX_ELEMENT_AMOUNT = 500;

    @Override
    public void registerConfig(Map<String, Class> outConfMap) {
        super.registerConfig(outConfMap);
        outConfMap.put(KEY_NUM, Integer.class);
        outConfMap.put(KEY_TABLE, String.class);
        outConfMap.put(KEY_POOL, String.class);
    }

    @Override
    public void recall(BaseDataCollection dc) {
        // 1.拿到个性化标签
        Set<UserProfile.Tag> profileTags = getProfileTags(dc);
        if (MXJudgeUtils.isEmpty(profileTags)) {
            return;
        }

        // 2.挑选真正使用的标签
        List<String> useTags = pickUseTags(profileTags);

        // 3.召回标签数据
        doRecall(dc, useTags);
    }

    /**
     * 召回标签数据
     */
    protected void doRecall(BaseDataCollection dc, List<String> useTags) {
        // 拿到池子配置
        String poolIndex = getPool();
        StrategyPoolConf poolConf = MXDataSource.strategyPool().getStrategyPoolConf(poolIndex);
        if (poolConf == null) {
            return;
        }

        // 召回
        LocalCacheDataSource cache = MXDataSource.cache();
        StrategyPoolExecutor executor = DataSourceManager.INSTANCE.getStrategyPoolExecutor();
        CountDownLatch cd = new CountDownLatch(useTags.size());
        String keyIt;
        List<BaseDocument> listIt;
        Map<String, List<BaseDocument>> map = new HashMap<>();
        String table = getTable();

        for (String tag : useTags) {
            keyIt = DefineTool.toKey(poolIndex, tag);

            // 先从缓存取
            listIt = cache.getTopHotTagDocumentCache(keyIt);
            if (MXJudgeUtils.isNotEmpty(listIt)) {
                map.put(keyIt, new ArrayList<>(listIt));
                cd.countDown();
                continue;
            }

            // 缓存没有，取现
            executor.execute(keyIt, map, poolConf.poolRecallSize, poolConf.priority, tag, table, cd);
        }

        try {
            cd.await(300, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            NewRelic.noticeError(getName() + ": Exception occurred when CountDownLatch.await(). " + e.getMessage());
            LogTool.reportError(DefineTool.ErrorEnum.GENERAL, logger, e);
        }

        // 存结果
        if (MXJudgeUtils.isNotEmpty(map)) {
            map.forEach((k, v) -> dc.syncSearchResultSizeMap.put(k, v.size()));
            setResult(dc, map);
        }
    }

    /**
     * 获取个性化标签
     */
    private Set<UserProfile.Tag> getProfileTags(BaseDataCollection dc) {
        return MXDataSource.profileStgTag().getData(
                dc.client.user.uuId,
                getTable(),
                UserProfileStrategyTagCA.COLUMN);
    }

    /**
     * 将个性化标签转换为用于提取数据的标签
     */
    private List<String> pickUseTags(Set<UserProfile.Tag> tags) {
        // 构建权重 tag 池
        List<String> tagPool = constructProbabilityTagPool(new ArrayList<>(tags));
        Collections.shuffle(tagPool);

        // 从池中取够数量
        Set<String> tagSet = new HashSet<>();
        int useTagNum = getNum();

        for (String name : tagPool) {
            if (tagSet.size() >= useTagNum) {
                break;
            }

            tagSet.add(name);
        }

        return new ArrayList<>(tagSet);
    }

    /**
     * 构建挑选 tag 池，设置不同 tag 的提取权重，方法是权重大的让它数量多
     */
    private List<String> constructProbabilityTagPool(List<UserProfile.Tag> tags) {
        List<String> list = new ArrayList<>();
        if (tags.size() <= getNum()) {
            for (UserProfile.Tag tag : tags) {
                list.add(tag.name);
            }
            return new ArrayList<>(list);
        }

        for (int i = 0; i < tags.size() && i < MAX_TAG_AMOUNT; i++) {
            int elementCount = 0;
            UserProfile.Tag tag = tags.get(i);
            while (elementCount < tag.score && elementCount < MAX_ELEMENT_AMOUNT) {
                list.add(tag.name);
                elementCount++;
            }
        }

        return list;
    }

    /**
     * 获取配置的 tag 数量，使用几个 tag 召回
     */
    public int getNum() {
        return config.getInt(KEY_NUM);
    }

    /**
     * 获取 tag CA table 配置，从哪个表召回 tag
     */
    public String getTable() {
        return config.getString(KEY_TABLE);
    }

    /**
     * 获取池子配置，从哪个池子召回视频数据
     */
    private String getPool() {
        return config.getString(KEY_POOL);
    }

    @Override
    public void fillDebugInfo(Map<String, String> outInfoMap, BaseDocument document) {
        outInfoMap.put("recall_tag", document.getRecallTag());
        outInfoMap.put("recall_table", document.getRecallTable());
    }
}
