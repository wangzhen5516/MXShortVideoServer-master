package mx.j2.recommend.recall.impl;

import com.newrelic.api.agent.Trace;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_source.LocalCacheDataSource;
import mx.j2.recommend.hystrix.redis.ZrevRangeWithScoresStragegyCommand;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.thrift.Location;
import mx.j2.recommend.thrift.Request;
import mx.j2.recommend.util.MXJudgeUtils;
import mx.j2.recommend.util.MXStringUtils;
import mx.j2.recommend.util.OptionalUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author: zhongren.li
 * @date: 2021-01-04
 */

public class StateLocationRecall extends BaseRecall<BaseDataCollection> {
    private static final int RECALL_SIZE = 500;
    private final String REDIS_KEY_FORMAT = "tophot_%s";
    private final String LOCAL_CACHE_PREFIX = "state_preference";

    private static final List<String> STATE_RANGE = new ArrayList<String>() {
        {
            add("maharashtra");
            add("national_capital_territory_of_delhi");
            add("uttar_pradesh");
            add("gujarat");
            add("rajasthan");
        }
    };

    @Override
    public boolean skip(BaseDataCollection data) {
        return false;
    }

    @Override
    @Trace(dispatcher = true)
    public void recall(BaseDataCollection dc) {

        final String[] stateArray = {""};
        OptionalUtil.ofNullable(dc.req)
                .getUtil(Request::getLocation)
                .getUtil(Location::getState)
                .ifPresent(state -> stateArray[0] = state);

        if (MXStringUtils.isEmpty(stateArray[0])) {
            return;
        }

        String stateString = getLowerState(stateArray[0]);
        if (!STATE_RANGE.contains(stateString)) {
            return;
        }

        LocalCacheDataSource localCacheDataSource = MXDataSource.cache();
        String localCacheKey = String.format("%s_%s", LOCAL_CACHE_PREFIX, stateString);
        List<BaseDocument> documents = localCacheDataSource.getTopHotTagDocumentCache(localCacheKey);
        if (MXJudgeUtils.isEmpty(documents)) {
            ZrevRangeWithScoresStragegyCommand command = new ZrevRangeWithScoresStragegyCommand(
                    String.format(REDIS_KEY_FORMAT, stateString), RECALL_SIZE);
            Map<String, Double> docWithScores = command.execute();
            if (MXJudgeUtils.isEmpty(docWithScores)) {
                return;
            }
            documents = MXDataSource.details().get(docWithScores.keySet(), this.getName());
            if (MXJudgeUtils.isEmpty(documents)) {
                return;
            }
            for (BaseDocument doc : documents) {
                if (docWithScores.containsKey(doc.id)) {
                    doc.scoreDocument.offlineCalculateScore = docWithScores.get(doc.id);
                }
            }
            documents.sort((o1, o2) -> Double.compare(o2.scoreDocument.offlineCalculateScore, o1.scoreDocument.offlineCalculateScore));
            if (MXJudgeUtils.isNotEmpty(documents)) {
                localCacheDataSource.setTopHotTagDocumentCache(localCacheKey, documents);
            }
        }
        dc.topHotStateList.addAll(documents);
    }

    private String getLowerState(String s) {
        return s.toLowerCase().replaceAll(" ", "_");
    }
}
