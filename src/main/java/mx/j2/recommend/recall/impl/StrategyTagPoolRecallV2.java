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
public class StrategyTagPoolRecallV2 extends BaseRecall<BaseDataCollection> {

    private final static Logger logger = LogManager.getLogger(StrategyTagPoolRecallV2.class);

    private static final int USE_TAG_NUMBER = 2;

    @Override
    public void init() {
    }

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
            if ("a".equals(dc.recommendFlow.name)) {
                dc.tagTableName = "up_human_tag_7d_v1";
            } else if ("b".equals(dc.recommendFlow.name)) {
                dc.tagTableName = "up_human_tag_7d_v2";
            }
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
        StrategyPoolConfDataSource strategyPoolConfDataSource = MXDataSource.strategyPool();
        Set<String> poolSet = strategyPoolConfDataSource.getPoolSet();
        if (CollectionUtils.isEmpty(poolSet)) {
            return;
        }

        List<Map<String, StrategyPoolConf>> mapList = new ArrayList<>();
        for (String pool: poolSet) {
            Map<String, StrategyPoolConf> needMap = new HashMap<>();
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
            mapList.add(needMap);
            dc.strategyPoolConfMap.putAll(needMap);
        }

        StrategyPoolExecutor executor = DataSourceManager.INSTANCE.getStrategyPoolExecutor();
        LocalCacheDataSource localCacheDataSource = MXDataSource.cache();
        mapList.forEach(e -> {
            if(null == e){
                return;
            }
            CountDownLatch cd = new CountDownLatch(e.size());
            e.forEach((k, v) ->{
                List<BaseDocument> documents = localCacheDataSource.getTopHotTagDocumentCache(k);
                if (CollectionUtils.isNotEmpty(documents)) {
                    dc.strategyPoolToDocumentListMap.put(k, documents);
                    cd.countDown();
                    return;
                }
                executor.execute(k, dc.strategyPoolToDocumentListMap, v.poolRecallSize, this.getName(), cd);
                try {
                    TimeUnit.MILLISECONDS.sleep(4);
                } catch (InterruptedException exception) {
                    exception.printStackTrace();
                }
            });
            try {
                cd.await(100, TimeUnit.MILLISECONDS);
            } catch (Exception exception) {
                NewRelic.noticeError("new error in counter await about strategy" + exception.getMessage());
                exception.printStackTrace();
                logger.error(exception.getMessage());
            }
        });

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
}
