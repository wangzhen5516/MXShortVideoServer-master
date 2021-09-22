package mx.j2.recommend.recall.impl;

import cn.hutool.core.collection.CollectionUtil;
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
public class StrategyTagPoolRecall extends BaseRecall<BaseDataCollection> {

    private final static Logger logger = LogManager.getLogger(StrategyTagPoolRecall.class);

    private static final int USE_TAG_NUMBER = 5;

    private String HUMAN_PREFIX = "human_tag_in_";
    private String AUDIO_PREFIX = "audio_";

    public static String[] SELECTED_TAG_LIST= {
            "funny",
            "comedy",
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
            "military"};

    @Override
    public boolean skip(BaseDataCollection data) {
        return false;
    }

    @Override
    public void recall(BaseDataCollection dc) {
        String localCacheKey = String.format("%s_%s", dc.client.user.uuId, "strategy_tag_pool");
        List<String> userTags = MXDataSource.cache().getUserTagsCache(localCacheKey);
        if (CollectionUtils.isEmpty(userTags)) {
            UserStrategyTagDataSource dataSource = MXDataSource.profileTagV2();
            setTagTable(dc);
            List<UserProfile.Tag> tags = dataSource.getTags(dc);
            if (CollectionUtil.isEmpty(tags)) {
                return;
            }
            userTags = getTags(tags);
            if (CollectionUtils.isEmpty(userTags)){
                return;
            }
            MXDataSource.cache().setUserTagsCache(localCacheKey, userTags);
        }

        processTags(userTags);

        doRecall(dc, userTags);
    }

    /**
     * 设置表名，抽出方法方便子类重写
     */
    protected void setTagTable(BaseDataCollection dc) {
        if ("mx_hot_tab_internal_version_2_0_C1".equals(dc.recommendFlow.name)
                || "mx_hot_tab_internal_version_2_0_C2".equals(dc.recommendFlow.name)) {
            dc.tagTableName = "up_human_tag_7d_v1";
        } else if ("mx_hot_tab_internal_version_2_0_E".equals(dc.recommendFlow.name)) {
            dc.tagTableName = "up_human_tag_7d_v2";
        }
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
        for (String pool: poolSet) {
            StrategyPoolConf conf = strategyPoolConfDataSource.getStrategyPoolConf(pool);
            if (null == conf) {
                continue;
            }

            if (!conf.excludeSmallFlowList.isEmpty() && conf.excludeSmallFlowList.contains(dc.recommendFlow.name)) {
                continue;
            }

            for (int i = 0; i < USE_TAG_NUMBER && i < userTags.size(); i++) {
                String key = String.format("%s_%s", conf.poolIndexPrefix, userTags.get(i));
                needMap.put(key, conf);
            }
        }
        sort(needMap, dc.strategyPoolConfMap);
        LocalCacheDataSource localCacheDataSource = MXDataSource.cache();
        StrategyPoolExecutor executor = DataSourceManager.INSTANCE.getStrategyPoolExecutor();
        CountDownLatch cd = new CountDownLatch(needMap.size());
        needMap.forEach((k, v) ->  {
            List<BaseDocument> documents = localCacheDataSource.getTopHotTagDocumentCache(k);
            if (CollectionUtils.isNotEmpty(documents)) {
                dc.strategyPoolToDocumentListMap.put(k, documents);
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
            dc.strategyPoolToDocumentListMap.forEach((k, v) -> dc.syncSearchResultSizeMap.put(k, v.size()));
        }
    }


    private List<String> getTags(List<UserProfile.Tag> tags) {
        List<String> list = new ArrayList<>();
        for (UserProfile.Tag value : tags) {
            if (list.size() >= USE_TAG_NUMBER) {
                break;
            }

            if (list.contains(value.name)) {
                continue;
            }

            list.add(value.name);
        }

        return new ArrayList<>(list);
    }

    protected void processTags(List<String> tags) {
        tags.removeIf(tag -> !isQualified(tag));
    }

    private boolean isQualified(String tag){
        if( tag.matches(HUMAN_PREFIX + "[\\w]*") ||tag.matches(AUDIO_PREFIX + "[\\w]*")){
            return true;
        }
        for(String selectedTag:SELECTED_TAG_LIST){
            if(selectedTag.equals(tag)){
                return true;
            }
        }
        return false;
    }
}
