package mx.j2.recommend.recall.impl;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_model.interfaces.IDocumentProcessor;
import mx.j2.recommend.data_source.StrategyElasticSearchDataSource;
import mx.j2.recommend.data_source.TopCityAndStateDateSource;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXJudgeUtils;
import mx.j2.recommend.util.MXStringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static mx.j2.recommend.data_source.NearByDataSource.*;
import static mx.j2.recommend.util.BaseMagicValueEnum.NEXT_TOKEN;

public class NearByLatitudeAndLongitudeRecall extends BaseRecall<BaseDataCollection> {

    private static final int RECALL_SIZE = 40;

    private static final String INDEX = "video_location_rollover_search";

    private static final String CITY_INDEX = "city_location_search_%s";

    private static final String STATE_INDEX = "state_location_search_%s";

    @Override
    public boolean skip(BaseDataCollection dc) {
        if (dc.req.location == null) {
            return true;
        }
        return false;
    }

    @Override
    public void recall(BaseDataCollection dc) {
        //构造es请求体
        JSONObject queryBody = constructNearByQuery(dc);
        int radius = 0;
        try {
            radius = parseNextToken(queryBody, dc);
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (radius <= 0) {
            return;
        }
        parseQuery(dc, queryBody, radius);

        //发送请求
        List<BaseDocument> asynRecallList = getResultByRequest(queryBody, dc, radius);

        //num记录循环次数
        int num = 1;
        //如果返回的结果为空，去掉search_after去下一环请求
        while (MXJudgeUtils.isEmpty(asynRecallList)) {
            //如果遍历所有环后列表仍为空，跳出循环
            if (++num > MAX_DISTANCE / STEP_SIZE) {
                break;
            }
            queryBody = constructNearByQuery(dc);
            radius = radius + STEP_SIZE > MAX_DISTANCE ? STEP_SIZE : radius + STEP_SIZE;
            parseQuery(dc, queryBody, radius);
            asynRecallList = getResultByRequest(queryBody, dc, radius);
        }

        dc.radius = radius;
        dc.asynRecallList.addAll(asynRecallList);
        dc.syncSearchResultSizeMap.put(this.getName(), asynRecallList.size());
        dc.resultFromMap.put(this.getName(), DefineTool.RecallFrom.ES.getName());
    }

    private List<BaseDocument> getResultByRequest(JSONObject queryBody, BaseDataCollection dc, int radius) {
        List<BaseDocument> asynRecallList = new ArrayList<>();

        String esIndex = INDEX;
        if (dc.req.location != null && MXJudgeUtils.isNotEmpty(dc.req.location.city)) {
            String cityRealName = getRealName(dc.req.location.city);
            if (TopCityAndStateDateSource.TOP_CITY_LIST.contains(cityRealName)) {
                esIndex = String.format(CITY_INDEX, cityRealName.replace(" ", "_"));
            }
        } else if (dc.req.location != null && MXJudgeUtils.isNotEmpty(dc.req.location.state)) {
            String stateRealName = getRealName(dc.req.location.state);
            if (TopCityAndStateDateSource.TOP_STATE_LIST.contains(stateRealName)) {
                esIndex = String.format(STATE_INDEX, stateRealName.replace(" ", "_"));
            }
        }

        StrategyElasticSearchDataSource strategyElasticSearchDataSource = MXDataSource.strategyES();
        //从es查询结果中获取视频id、距离和热度信息，保存在map中
        JSONObject jsonObj = strategyElasticSearchDataSource.getHeatScoreAndDistanceFromES(queryBody.toString(), esIndex);
        if (jsonObj.isEmpty()) {
            return asynRecallList;
        }

        JSONObject heatScoreAndDistanceJSON = jsonObj.getJSONObject("heatScore");
        Map<String, String> nextTokenMap = jsonObj.getObject("tokenMap", Map.class);
        dc.nextTokenMap.put(dc.req.interfaceName, nextTokenMap);

        IDocumentProcessor processor = document -> {
            JSONObject heatScoreAndDistance = heatScoreAndDistanceJSON.getJSONObject(document.id);
            if (heatScoreAndDistance != null) {
                //将距离和热度信息保存在document的字段中
                document.onlineTimeNeed = heatScoreAndDistance.getLongValue("onlineTimeNeed");
                document.multipleScore = heatScoreAndDistance.getDoubleValue("heatScore");
                document.distance = heatScoreAndDistance.getDoubleValue("distance");
                if (MXJudgeUtils.isNotEmpty(nextTokenMap) && MXJudgeUtils.isNotEmpty(nextTokenMap.get(document.id))) {
                    String token = String.format("%s|%s", nextTokenMap.get(document.id), radius);
                    document.nextTokenMap.put(dc.req.interfaceName, token);
                }
                asynRecallList.add(document);
            }
        };

        //从cassandra中获取视频信息
        return MXDataSource.details().get(heatScoreAndDistanceJSON.keySet(), getName(), processor);
    }

    /**
     * 构造es请求中的sort
     *
     * @param dc
     * @return
     */
    private JSONObject constructNearByQuery(BaseDataCollection dc) {
        JSONArray sortArray = new JSONArray();
        JSONObject sortObject = new JSONObject();
        JSONObject _geo_distance = new JSONObject();
        JSONObject location_sort = new JSONObject();
        JSONObject idSort = new JSONObject();
        JSONObject id_sort = new JSONObject();

        JSONObject onlineTime = new JSONObject();
        JSONObject onlineTimeOrder = new JSONObject();

        JSONObject heatScore = new JSONObject();
        JSONObject heatScoreOrder = new JSONObject();

        //heat_score和online_time加权分数召回暂时下掉  2020/12/8
        /*JSONObject heatAndTimeSortFather = new JSONObject();
        JSONObject heatAndTimeSort = new JSONObject();
        JSONObject script = new JSONObject();
        JSONObject params = new JSONObject();*/

        JSONObject content = new JSONObject();

        onlineTimeOrder.put("order", "desc");
        onlineTimeOrder.put("missing", 0);
        onlineTime.put("online_time_day", onlineTimeOrder);

        heatScoreOrder.put("order", "desc");
        heatScoreOrder.put("missing", 0);
        heatScore.put("heat_score", heatScoreOrder);

        location_sort.put("lat", dc.req.location.coordinateX);
        location_sort.put("lon", dc.req.location.coordinateY);
        _geo_distance.put("location", location_sort);
        _geo_distance.put("unit", "km");
        _geo_distance.put("distance_type", "arc");
        _geo_distance.put("order", "asc");
        _geo_distance.put("validation_method", "STRICT");
        sortObject.put("_geo_distance", _geo_distance);

        id_sort.put("order", "asc");
        idSort.put("_id", id_sort);

        /*params.put("heat_score_conf", HEAT_SCORE_CONF);
        params.put("online_time_conf", ONLINE_TIME_CONF);
        script.put("lang", "painless");
        script.put("source", "(doc['heat_score'].size()==0?0:doc['heat_score'].value*params.heat_score_conf) + (doc['online_time'].size()==0?0:doc['online_time'].value*params.online_time_conf)");
        script.put("params", params);
        heatAndTimeSort.put("type", "number");
        heatAndTimeSort.put("script", script);
        heatAndTimeSort.put("order", "desc");
        heatAndTimeSortFather.put("_script", heatAndTimeSort);*/

        sortArray.add(onlineTime);
        sortArray.add(heatScore);
        sortArray.add(sortObject);
        sortArray.add(idSort);

        content.put("sort", sortArray);
        content.put("size", RECALL_SIZE);
        return content;
    }

    /**
     * 解析nextToken
     *
     * @param queryBody
     * @param dc
     * @return
     */
    private int parseNextToken(JSONObject queryBody, BaseDataCollection dc) {
        //nextToken为空说明第一次进来
        if (MXStringUtils.isEmpty(dc.req.nextToken)) {
            return START_RADIUS;
        }

        JSONArray result = new JSONArray();
        String[] tokens = MXStringUtils.split(dc.req.nextToken, "|");
        if (5 > tokens.length) {
            return START_RADIUS;
        }

        long token_1 = Long.parseLong(tokens[0]);
        double token_2 = Double.parseDouble(tokens[1]);
        double token_3 = Double.parseDouble(tokens[2]);
        String token_4 = tokens[3];
        int token_5 = Integer.parseInt(tokens[4]);
        //如果nextToken中的第四个参数为next_token，表示进入下一环或从头开始查询es
        if (token_4.equals(NEXT_TOKEN)) {
            return token_5 + STEP_SIZE > MAX_DISTANCE ? START_RADIUS : token_5 + STEP_SIZE;
        }
        result.add(token_1);
        result.add(token_2);
        result.add(token_3);
        result.add(token_4);

        queryBody.put("search_after", result);
        return token_5;
    }

    /**
     * 构造es请求中的query
     *
     * @param dc
     * @param queryBody
     * @param radius
     */
    private void parseQuery(BaseDataCollection dc, JSONObject queryBody, int radius) {
        JSONObject location_high = new JSONObject();
        JSONObject geo_distance_high = new JSONObject();
        JSONObject filter_high = new JSONObject();
        JSONObject constant_score_high = new JSONObject();
        JSONObject must = new JSONObject();
        JSONObject bool = new JSONObject();
        JSONObject query = new JSONObject();

        location_high.put("lat", dc.req.location.coordinateX);
        location_high.put("lon", dc.req.location.coordinateY);
        geo_distance_high.put("distance", radius + "km");
        geo_distance_high.put("location", location_high);
        filter_high.put("geo_distance", geo_distance_high);
        constant_score_high.put("filter", filter_high);
        must.put("constant_score", constant_score_high);
        bool.put("must", must);

        //当前es版本不再支持geo_distance_range,暂时用must和must_not替代，因为半径必须大于0，所以radius>STEP_SIZE时才有must_not
        if (radius > STEP_SIZE) {
            JSONObject location_low = new JSONObject();
            JSONObject geo_distance_low = new JSONObject();
            JSONObject filter_low = new JSONObject();
            JSONObject constant_score_low = new JSONObject();
            JSONObject must_not = new JSONObject();

            location_low.put("lat", dc.req.location.coordinateX);
            location_low.put("lon", dc.req.location.coordinateY);
            geo_distance_low.put("distance", radius - STEP_SIZE + "km");
            geo_distance_low.put("location", location_low);
            filter_low.put("geo_distance", geo_distance_low);
            constant_score_low.put("filter", filter_low);
            must_not.put("constant_score", constant_score_low);
            bool.put("must_not", must_not);
        }
        query.put("bool", bool);
        queryBody.put("query", query);
    }

    private String getRealName(String name) {
        return name.toLowerCase().replace("+", " ");
    }

}
