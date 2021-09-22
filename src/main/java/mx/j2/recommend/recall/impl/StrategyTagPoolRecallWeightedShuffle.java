package mx.j2.recommend.recall.impl;

import com.newrelic.api.agent.NewRelic;
import mx.j2.recommend.data_model.UserProfile;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_source.LocalCacheDataSource;
import mx.j2.recommend.data_source.StrategyPoolConfDataSource;
import mx.j2.recommend.data_source.UserStrategyTagDataSource;
import mx.j2.recommend.manager.DataSourceManager;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.pool_conf.StrategyPoolConf;
import mx.j2.recommend.task.StrategyPoolExecutor;
import mx.j2.recommend.util.MXCollectionUtils;
import mx.j2.recommend.util.MXJudgeUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @author ：zhongrenli
 * @date ：Created in 9:44 下午 2021/2/1
 */
public class StrategyTagPoolRecallWeightedShuffle extends BaseRecall<BaseDataCollection> {

    private final static Logger logger = LogManager.getLogger(StrategyTagPoolRecallWeightedShuffle.class);

    int USE_TAG_NUMBER = 4;
    private static final int MAX_TAG_AMOUNT = 100;
    private static final int MAX_ELEMENT_AMOUNT = 500;

    private static final String HUMAN_PREFIX = "human_tag_in_";
    private static final String AUDIO_PREFIX = "audio_";
    private static final Set<String> SELECTED_TAG_SET = new HashSet<>(Arrays.asList(
            "food",
            "movie",
            "lifehack",
            "dog",
            "pet",
            "pubg",
            "goodthing",
            "travel",
            "gongzuo",
            "baby",
            "wwe",
            "makeup",
            "paint",
            "car",
            "tech",
            "edu",
            "cricket",
            "adventure",
            "cat",
            "wildlife",
            "science",
            "bts",
            "diy",
            "ipl",
            "nail",
            "yoga",
            "engineer",
            "yogagirl",
            "football",
            "skiing",
            "kpop",
            "japan",
            "military"));

    @Override
    public boolean skip(BaseDataCollection data) {
        return false;
    }

    @Override
    public void recall(BaseDataCollection dc) {
        UserStrategyTagDataSource dataSource = MXDataSource.profileTagV2();
        setTagTable(dc);
        List<UserProfile.Tag> tags = dataSource.getTags(dc);
        if (MXJudgeUtils.isEmpty(tags)) {
            return;
        }
        processTags(tags);
        List<String> userTags = convertTags(tags, USE_TAG_NUMBER);
        if (CollectionUtils.isEmpty(userTags)) {
            return;
        }
        doRecall(dc, userTags);
    }

    /**
     * 设置表名，抽出方法方便子类重写
     */
    protected void setTagTable(BaseDataCollection dc) {
        dc.tagTableName = "up_ml_tag_60d_v1";
    }

    /**
     * 抽出召回过程方便子类重写
     */
    protected void doRecall(BaseDataCollection dc, List<String> userTags) {
        StrategyPoolConfDataSource strategyPoolConfDataSource = MXDataSource.strategyPool();
        Set<String> poolSet = strategyPoolConfDataSource.getPoolSet();
        if (CollectionUtils.isEmpty(poolSet)) {
            return;
        }

        Map<String, StrategyPoolConf> needMap = new HashMap<>();
        for (String pool : poolSet) {
            StrategyPoolConf conf = strategyPoolConfDataSource.getStrategyPoolConf(pool);
            if (null == conf) {
                continue;
            }

            if (!conf.excludeSmallFlowList.isEmpty() && conf.excludeSmallFlowList.contains(dc.recommendFlow.name)) {
                continue;
            }
            for (String tagName : userTags) {
                String key = String.format("%s_%s", conf.poolIndexPrefix, tagName);
                needMap.put(key, conf);
            }
        }
        sort(needMap, dc.strategyPoolConfMap);
        LocalCacheDataSource localCacheDataSource = MXDataSource.cache();
        StrategyPoolExecutor executor = DataSourceManager.INSTANCE.getStrategyPoolExecutor();
        CountDownLatch cd = new CountDownLatch(needMap.size());
        needMap.forEach((k, v) -> {
            List<BaseDocument> documents = localCacheDataSource.getTopHotTagDocumentCache(k);
            if (CollectionUtils.isNotEmpty(documents)) {
                dc.strategyPoolToDocumentListMap.put(k, new ArrayList<>(documents));
                cd.countDown();
                return;
            }
            executor.execute(k, dc.strategyPoolToDocumentListMap, v.poolRecallSize, this.getName(), cd);
        });
        try {
            cd.await(300, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            NewRelic.noticeError("new error in counter await about maxWaitTimeMs" + e.getMessage());
            e.printStackTrace();
            logger.error(e.getMessage());
        }
        if (!dc.strategyPoolToDocumentListMap.isEmpty()) {
            merge(dc);
            dc.strategyPoolToDocumentListMap.forEach((k, v) -> dc.syncSearchResultSizeMap.put(k, v.size()));
        }
    }

    void merge(BaseDataCollection dc) {
        Map<String, List<BaseDocument>> map = new HashMap<>();
        dc.strategyPoolConfMap.forEach((k, v) -> {
            List<BaseDocument> documents = dc.strategyPoolToDocumentListMap.get(k);
            if (MXCollectionUtils.isEmpty(documents)) {
                return;
            }
            documents.forEach(doc -> {
                doc.setStrategyPoolPriority(v.priority);
            });

            String tag = k.split("_")[k.split("_").length-1];
            if (map.containsKey(tag)) {
                map.get(tag).addAll(documents);
            } else {
                map.put(tag, new ArrayList<>(documents));
            }
        });

        map.forEach((k, v) -> {
            if (CollectionUtils.isEmpty(v)) {
                return;
            }
            v.sort(Comparator.comparingInt(BaseDocument::getStrategyPoolPriority).reversed());
            dc.strategyTagDocumentListMap.put(k, v);
        });
    }

    private static List<String> constructProbabilityList(List<UserProfile.Tag> tags, int useTagNumber) {
        List<String> list = new ArrayList<>();
        if (tags.size() <= useTagNumber) {
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

    public static List<String> convertTags(List<UserProfile.Tag> tags, int useTagNumber) {
        List<String> userTags = constructProbabilityList(tags, useTagNumber);
        return getTags(userTags, useTagNumber);
    }

    private static List<String> getTags(List<String> list, int useTagNum) {
        Collections.shuffle(list);
        Set<String> resSet = new HashSet<>();
        for (String name : list) {
            if (resSet.size() >= useTagNum) {
                break;
            }

            resSet.add(name);
        }
        return new ArrayList<>(resSet);
    }

    protected void processTags(List<UserProfile.Tag> tags) {
        tags.removeIf(tag -> !isQualified(tag));
    }

    protected boolean isQualified(UserProfile.Tag tag) {
        if (tag.name.matches(HUMAN_PREFIX + "[\\w]*") || tag.name.matches(AUDIO_PREFIX + "[\\w]*")) {
            return true;
        }

        return SELECTED_TAG_SET.contains(tag.name);
    }
}
