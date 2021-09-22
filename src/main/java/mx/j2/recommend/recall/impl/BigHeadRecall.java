package mx.j2.recommend.recall.impl;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_source.StrategyElasticSearchDataSource;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXJudgeUtils;

import java.util.List;
import java.util.Random;

/**
 * @author liangjie.feng
 * @version 1.0
 * @date 2021/1/19 下午4:29
 * @description
 */
public class BigHeadRecall extends BaseRecall<BaseDataCollection> {

    private static final int RECALL_SIZE = 50;

    private static final String INDEX = "video_big_head";

    private static final Random RANDOM = new Random();

    private static final float PERCENTAGE = 0.15f;

    @Override
    public boolean skip(BaseDataCollection dc) {
        //此路召回以0.15的概率混入
        if (dc.req.location == null || dc.req.location.coordinateX == 0 || dc.req.location.coordinateY == 0
                || RANDOM.nextFloat() > PERCENTAGE) {
            return true;
        }
        return false;
    }

    @Override
    public void recall(BaseDataCollection dc) {
        JSONObject queryBody = constructNearByQuery(dc);
        parseQuery(dc, queryBody);

        StrategyElasticSearchDataSource strategyElasticSearchDataSource = MXDataSource.strategyES();
        List<BaseDocument> resultList = strategyElasticSearchDataSource.syncLoadBySearchQyery(queryBody.toString(), INDEX, this.getName());
        if (MXJudgeUtils.isEmpty(resultList)) {
            return;
        }

        dc.bigHeadList.addAll(resultList);
        dc.syncSearchResultSizeMap.put(this.getName(), resultList.size());
        dc.resultFromMap.put(this.getName(), DefineTool.RecallFrom.ES.getName());
    }

    /**
     * 构造es请求中的sort和size
     * @param dc
     * @return
     */
    private JSONObject constructNearByQuery(BaseDataCollection dc) {
        JSONArray sortArray = new JSONArray();
        JSONObject sortObject = new JSONObject();
        JSONObject geoDistance = new JSONObject();
        JSONObject locationSort = new JSONObject();
        JSONObject idSort = new JSONObject();
        JSONObject idOrder = new JSONObject();

        JSONObject content = new JSONObject();

        locationSort.put("lat", dc.req.location.coordinateX);
        locationSort.put("lon", dc.req.location.coordinateY);
        geoDistance.put("location", locationSort);
        geoDistance.put("unit", "km");
        geoDistance.put("distance_type", "arc");
        geoDistance.put("order", "asc");
        geoDistance.put("validation_method", "STRICT");
        sortObject.put("_geo_distance", geoDistance);

        idOrder.put("order", "asc");
        idSort.put("_id", idOrder);

        sortArray.add(sortObject);
        sortArray.add(idSort);

        content.put("sort", sortArray);
        content.put("size", RECALL_SIZE);
        return content;
    }

    /**
     * 构造es请求中的query
     * @param dc
     * @param queryBody
     */
    private void parseQuery(BaseDataCollection dc, JSONObject queryBody) {
        JSONObject location_high = new JSONObject();
        JSONObject geo_distance_high = new JSONObject();
        JSONObject filter_high = new JSONObject();
        JSONObject constant_score_high = new JSONObject();
        JSONObject must = new JSONObject();
        JSONObject bool = new JSONObject();
        JSONObject query = new JSONObject();

        JSONObject filter = new JSONObject();
        JSONObject range = new JSONObject();
        JSONObject avgFaceRatio = new JSONObject();

        location_high.put("lat", dc.req.location.coordinateX);
        location_high.put("lon", dc.req.location.coordinateY);
        geo_distance_high.put("distance", "5km");
        geo_distance_high.put("location", location_high);
        filter_high.put("geo_distance", geo_distance_high);
        constant_score_high.put("filter", filter_high);
        must.put("constant_score", constant_score_high);
        bool.put("must", must);

        avgFaceRatio.put("lt", 0.1);
        range.put("avg_face_ratio", avgFaceRatio);
        filter.put("range", range);
        bool.put("filter", filter);

        query.put("bool", bool);
        queryBody.put("query", query);
    }
}
