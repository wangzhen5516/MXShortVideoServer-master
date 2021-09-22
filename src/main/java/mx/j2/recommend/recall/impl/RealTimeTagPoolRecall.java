package mx.j2.recommend.recall.impl;

import com.newrelic.api.agent.NewRelic;
import mx.j2.recommend.data_model.UserProfile;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_source.LocalCacheDataSource;
import mx.j2.recommend.data_source.StrategyPoolConfDataSource;
import mx.j2.recommend.data_source.UserProfileTagDataSource;
import mx.j2.recommend.manager.DataSourceManager;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.pool_conf.StrategyPoolConf;
import mx.j2.recommend.task.StrategyPoolExecutor;
import mx.j2.recommend.util.MXJudgeUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author Qi Mao
 * @date 2/19/2021
 * 结合了两种recall，因为只能单继承，所以重新写一个recall！
 */

public class RealTimeTagPoolRecall extends BaseRecall<BaseDataCollection>{
    private String HUMAN_PREFIX = "human_tag_in_";
    private String AUDIO_PREFIX = "audio_";
    int TAG_NUM = 6;
    private static Logger logger = LogManager.getLogger(RealTimeTagPoolRecall.class);
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
        // 借助标签数据源工具类去获取标签
        UserProfileTagDataSource dataSource = MXDataSource.profileTag();
        List<UserProfile.Tag> tags = dataSource.getTags(dc);

        // 木有标签，啥也不干
        if (MXJudgeUtils.isEmpty(tags)) {
            return;
        }

        /*
         * 过滤、不排序，但是截断前三个，打混随机
         */
        try {
            tags = tags.stream().filter(tag -> tag.score > 3).collect(Collectors.toList());
            processTags(tags);
        } catch (Exception e) {
            e.printStackTrace();
        }

        List<String> stringTags = StrategyTagPoolRecallWeightedShuffle.convertTags(tags, TAG_NUM);

        doRecall(dc, stringTags);
    }

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

            for (int i = 0; i < TAG_NUM && i < userTags.size(); i++) {
                String key = String.format("%s_%s", conf.poolIndexPrefix, userTags.get(i));
                needMap.put(key, conf);
            }
        }
        dc.realTimeStrategyPoolConfMap.putAll(needMap);

        LocalCacheDataSource localCacheDataSource = MXDataSource.cache();

        StrategyPoolExecutor executor = DataSourceManager.INSTANCE.getStrategyPoolExecutor();
        CountDownLatch cd = new CountDownLatch(needMap.size());
        needMap.forEach((k, v) ->  {
            List<BaseDocument> documents = localCacheDataSource.getTopHotTagDocumentCache(k);
            if (CollectionUtils.isNotEmpty(documents)) {
                dc.realTimeStrategyPoolToDocumentListMap.put(k, documents);
                cd.countDown();
                return;
            }
            executor.execute(k, dc.realTimeStrategyPoolToDocumentListMap, v.poolRecallSize, this.getName(), cd);
        });
        try {
            cd.await(300, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            NewRelic.noticeError("new error in counter await about maxWaitTimeMs" + e.getMessage());
            e.printStackTrace();
            logger.error(e.getMessage());
        }
        if (!dc.realTimeStrategyPoolToDocumentListMap.isEmpty()) {
            dc.realTimeStrategyPoolToDocumentListMap.forEach((k, v) -> dc.syncSearchResultSizeMap.put(k, v.size()));
        }

    }

    protected void processTags(List<UserProfile.Tag> tags) {
        tags.removeIf(tag -> !isQualified(tag));
    }

    private boolean isQualified(UserProfile.Tag tag){
        if( tag.name.matches(HUMAN_PREFIX + "[\\w]*") ||tag.name.matches(AUDIO_PREFIX + "[\\w]*")){
            return true;
        }
        for(String selectedTag:SELECTED_TAG_LIST){
            if(selectedTag.equals(tag.name)){
                return true;
            }
        }
        return false;
    }

    private List<String> getTags(List<UserProfile.Tag> tags) {
        List<String> list = new ArrayList<>();
        for (UserProfile.Tag value : tags) {
            if (list.size() >= TAG_NUM) {
                break;
            }

            if (list.contains(value.name)) {
                continue;
            }

            list.add(value.name);
        }

        return new ArrayList<>(list);
    }
}
