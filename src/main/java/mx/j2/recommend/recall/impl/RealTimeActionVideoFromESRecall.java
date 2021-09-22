package mx.j2.recommend.recall.impl;

import com.alibaba.fastjson.JSONObject;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_source.StrategyElasticSearchDataSource;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXCollectionUtils;
import mx.j2.recommend.util.MXJudgeUtils;
import mx.j2.recommend.util.MXStringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 根据最近点赞的视频，用ES7中的video_vector字段进行相似视频匹配（同LatestViewRelatedFromESRecall一并使用）
 *
 * @author DuoZhao
 * @date 2020/12/15
 */
@Deprecated
public class RealTimeActionVideoFromESRecall extends BaseRecall<BaseDataCollection> {
    private static Logger logger = LogManager.getLogger(RealTimeActionVideoFromESRecall.class);
    private final static int RECALL_SIZE = 5;
    private final static String INDEX_URL = DefineTool.CategoryEnum.DENSE_VECTOR_VIDEO.getIndex();
    private final static StrategyElasticSearchDataSource ELASTIC_SEARCH_DATA_SOURCE = MXDataSource.strategyES();

    @Override
    public boolean skip(BaseDataCollection data) {
        return false;
    }

    @Override
    public void recall(BaseDataCollection dc) {
        if (null == dc.req.extraClientInfo || MXStringUtils.isEmpty(dc.req.extraClientInfo.lastInteractiveId)) {
            return;
        }
        if (MXStringUtils.isBlank(dc.req.extraClientInfo.lastInteractiveType)) {
            return;
        }

        List<String> type = dc.recommendFlow.realType;
        if (MXCollectionUtils.isEmpty(type)) {
            return;
        }
        if (!type.contains(dc.req.extraClientInfo.lastInteractiveType)) {
            return;
        }
        List<Double> vectors = getVectorOfTheVideo(dc.req.extraClientInfo.lastInteractiveId);
        if (MXJudgeUtils.isEmpty(vectors)) {
            return;
        }

        JSONObject query = constructSimilarQuery(vectors);
        if (query.isEmpty()) {
            return;
        }

        List<BaseDocument> esDocList = ELASTIC_SEARCH_DATA_SOURCE.syncLoadBySearchQyery(query.toString(), INDEX_URL, this.getName());
        if (null == esDocList) {
            logger.error("get no result from es,lastInteractiveId:" + dc.req.extraClientInfo.lastInteractiveId);
            return;
        }

        List<String> idList = esDocList.stream().map(BaseDocument::getId).collect(Collectors.toList());
        if (MXJudgeUtils.isEmpty(idList)) {
            return;
        }

        List<BaseDocument> docList = MXDataSource.details().get(idList, this.getName());

        if (MXJudgeUtils.isNotEmpty(docList)) {
            docList.removeIf(item -> dc.req.extraClientInfo.lastInteractiveId.equals(item.id));
            dc.realTimeClickDocList.addAll(docList);
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
