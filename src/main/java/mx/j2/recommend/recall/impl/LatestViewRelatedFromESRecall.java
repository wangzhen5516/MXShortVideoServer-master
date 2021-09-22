package mx.j2.recommend.recall.impl;

import com.alibaba.fastjson.JSONObject;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_source.StrategyElasticSearchDataSource;
import mx.j2.recommend.hystrix.redis.ZrevRangePvCommand;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXJudgeUtils;
import mx.j2.recommend.util.MXStringUtils;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 根据最近观看的视频，用ES7中的video_vector字段进行相似视频匹配（同RealTimeActionVideoFromESRecall一并使用）
 * embeddinng
 * @author DuoZhao
 * @date 2020/12/15
 */
@Deprecated
public class LatestViewRelatedFromESRecall extends BaseRecall<BaseDataCollection> {
    private static final String REDIS_KEY_FORMAT = "%s:prefer_video_v1";
    private static final int REDIS_END = 0;
    private final static int RECALL_SIZE = 100;
    private final static String INDEX_URL = DefineTool.CategoryEnum.DENSE_VECTOR_VIDEO.getIndex();
    private final static StrategyElasticSearchDataSource ELASTIC_SEARCH_DATA_SOURCE = MXDataSource.strategyES();

    @Override
    public boolean skip(BaseDataCollection data) {
        return false;
    }

    @Override
    public void recall(BaseDataCollection dc) {
        if (MXStringUtils.isEmpty(dc.client.user.uuId)) {
            return;
        }

        ZrevRangePvCommand zrevRangePvCommand = new ZrevRangePvCommand(String.format(REDIS_KEY_FORMAT, dc.client.user.uuId), 0, REDIS_END);
        List<String> videoId = zrevRangePvCommand.execute();

        if (MXJudgeUtils.isEmpty(videoId)) {
            return;
        }

        List<Double> vectors = getVectorOfTheVideo(videoId.get(0));
        if (MXJudgeUtils.isEmpty(vectors)) {
            return;
        }

        JSONObject query = constructSimilarQuery(vectors);
        if (query.isEmpty()) {
            return;
        }

        List<BaseDocument> esDocList = ELASTIC_SEARCH_DATA_SOURCE.syncLoadBySearchQyery(query.toString(), INDEX_URL, this.getName());
        if (null == esDocList) {
            return;
        }

        List<String> idList = esDocList.stream().map(BaseDocument::getId).collect(Collectors.toList());
        if (MXJudgeUtils.isEmpty(idList)) {
            return;
        }

        List<BaseDocument> docList = MXDataSource.details().get(idList, this.getName());

        docList.sort((d1, d2) -> {
            if(d1.heatScore2 > d2.heatScore2) {
                return -1;
            }
            else {
                return 1;
            }
        });

        if (MXJudgeUtils.isNotEmpty(docList)) {
            docList.removeIf(item -> videoId.get(0).equals(item.id));
            dc.latestViewRelatedDocList.addAll(docList);
            dc.syncSearchResultSizeMap.put(this.getName(), docList.size());
            dc.resultFromMap.put(this.getName(), DefineTool.RecallFrom.ES.getName());
        }
    }

    private List<Double> getVectorOfTheVideo(String id) {
        List<JSONObject> objects = ELASTIC_SEARCH_DATA_SOURCE.syncLoadDetailBySearch(
                1,
                Collections.singletonList(id),
                INDEX_URL, null);

        if (MXJudgeUtils.isEmpty(objects)) {
            return null;
        }

        JSONObject o = objects.get(0);
        if (o.containsKey("video_vector")) {
            return o.getJSONArray("video_vector").toJavaList(Double.class);
        }
        return null;
    }

    private JSONObject constructSimilarQuery(List<Double> queryVector) {
        JSONObject query = new JSONObject();

        JSONObject scriptScoreObject = new JSONObject();
        JSONObject subQuery = new JSONObject();
        JSONObject script = new JSONObject();
        JSONObject params = new JSONObject();

        params.put("query_vector", queryVector);
        script.put("source", getCosFunctionString());
        script.put("params", params);

        subQuery.put("match_all", new JSONObject());
        scriptScoreObject.put("query", subQuery);
        scriptScoreObject.put("script", script);

        JSONObject scriptQuery = new JSONObject();
        scriptQuery.put("script_score", scriptScoreObject);

        query.put("size", RECALL_SIZE);
        query.put("query", scriptQuery);

        return query;
    }

    private String getCosFunctionString() {
        return "cosineSimilarity(params.query_vector, doc['video_vector']) + 1.0";
    }

}
