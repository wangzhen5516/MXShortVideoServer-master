package mx.j2.recommend.recall.impl;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import mx.j2.recommend.component.configurable.base.BaseConfigurableRecall;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.pool_conf.StrategyPoolConf;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXJudgeUtils;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author zhongrenli
 */
public abstract class BaseRecall<T extends BaseDataCollection> extends BaseConfigurableRecall<T> {
    JSONObject uidSortObj;
    String requestUrlFormat = "/%s/_search?pretty=false";
    private static final int DURATION_FILTER_MILLSECONDS = 3000;

    /**
     * 召回器权重分数访问器
     * float 类型，便于微调分数
     */
    @Override
    public float getRecallWeightScore() {
        return 0;
    }

    /**
     * 构造函数
     */
    public BaseRecall() {
        JSONObject sortCore2 = new JSONObject();
        sortCore2.put("order", "asc");
        uidSortObj = new JSONObject();
        uidSortObj.put("_uid", sortCore2);
        init();
    }

    /**
     * 打分器初始化
     */
    public void init() {
    }

    @Override
    public void doWork(T dc) {
        recall(dc);
    }

    /**
     * 返回需要的调试信息，各子类覆写
     */
    public void fillDebugInfo(Map<String, String> outInfoMap, BaseDocument document) {

    }


    /**
     * 构造保底相应类别的请求query
     */
    JSONObject constructQuery(T baseDc) {
        JSONObject query = new JSONObject();
        JSONObject bool = new JSONObject();
        JSONArray must = new JSONArray();
        JSONArray mustNot = new JSONArray();

        JSONObject matchFather = new JSONObject();
        JSONObject match = new JSONObject();
        match.put("status", DefineTool.OnlineStatusesEnum.ONLINE.getIndex());
        matchFather.put("match", match);
        must.add(matchFather);

        JSONArray filter = getFilterCondition(baseDc);
        if (MXJudgeUtils.isNotEmpty(filter)) {
            bool.put("filter", filter);
        }

        if (DefineTool.TabInfoEnum.GIF.getId().equals(baseDc.req.getTabId())) {
            JSONObject rangeFather = new JSONObject();
            JSONObject field = new JSONObject();
            JSONObject range = new JSONObject();
            range.put("gte", DURATION_FILTER_MILLSECONDS);
            field.put("target_gif_info.duration", range);
            rangeFather.put("range", field);
            must.add(rangeFather);
        }

        JSONObject specialNotLikeMatchFather = new JSONObject();
        JSONObject speciaNotLikelMatch = new JSONObject();
        speciaNotLikelMatch.put("special_sign", -1);
        specialNotLikeMatchFather.put("match", speciaNotLikelMatch);
        mustNot.add(specialNotLikeMatchFather);

        // 漂流瓶视频不可以召回
        JSONObject notBottleMatchFather = new JSONObject();
        JSONObject notBottleMatch = new JSONObject();
        notBottleMatch.put("is_bottle", 1);
        notBottleMatchFather.put("match", notBottleMatch);
        mustNot.add(notBottleMatchFather);

        bool.put("must", must);
        bool.put("must_not", mustNot);
        query.put("bool", bool);

        return query;
    }

    /**
     * 构造运营标记的相应类别的请求query
     */
    JSONObject constructQueryWithOperate(T baseDc) {
        JSONObject query = new JSONObject();
        JSONObject bool = new JSONObject();
        JSONArray must = new JSONArray();
        JSONArray mustNot = new JSONArray();

        JSONObject onlineMatchFather = new JSONObject();
        JSONObject onlineMatch = new JSONObject();
        onlineMatch.put("status", DefineTool.OnlineStatusesEnum.ONLINE.getIndex());
        onlineMatchFather.put("match", onlineMatch);
        must.add(onlineMatchFather);

        // 漂流瓶视频不可以召回
        JSONObject notBottleMatchFather = new JSONObject();
        JSONObject notBottleMatch = new JSONObject();
        notBottleMatch.put("is_bottle", 1);
        notBottleMatchFather.put("match", notBottleMatch);
        mustNot.add(notBottleMatchFather);

        if (DefineTool.TabInfoEnum.GIF.getId().equals(baseDc.req.getTabId())) {
            JSONObject rangeFather = new JSONObject();
            JSONObject field = new JSONObject();
            JSONObject range = new JSONObject();
            range.put("gte", DURATION_FILTER_MILLSECONDS);
            field.put("target_gif_info.duration", range);
            rangeFather.put("range", field);
            must.add(rangeFather);
        }

        JSONArray filter = getFilterCondition(baseDc);
        if (MXJudgeUtils.isNotEmpty(filter)) {
            bool.put("filter", filter);
        }

        bool.put("must", must);
        bool.put("must_not", mustNot);
        query.put("bool", bool);

        return query;
    }

    /**
     * 构造相应类别的请求query
     */
    JSONObject constructQueryWithDecrypt(T baseDc) {
        JSONObject query = new JSONObject();
        JSONObject bool = new JSONObject();
        JSONArray must = new JSONArray();
        JSONArray mustNot = new JSONArray();

        JSONObject onlineMatchFather = new JSONObject();
        JSONObject onlineMatch = new JSONObject();
        onlineMatch.put("status", DefineTool.OnlineStatusesEnum.ONLINE.getIndex());
        onlineMatchFather.put("match", onlineMatch);
        must.add(onlineMatchFather);

        JSONObject decryptMatchFather = new JSONObject();
        JSONObject decryptMatch = new JSONObject();
        decryptMatch.put("decrypt_sign", 1);
        decryptMatchFather.put("match", decryptMatch);
        must.add(decryptMatchFather);

        // 漂流瓶视频不可以召回
        JSONObject notBottleMatchFather = new JSONObject();
        JSONObject notBottleMatch = new JSONObject();
        notBottleMatch.put("is_bottle", 1);
        notBottleMatchFather.put("match", notBottleMatch);
        mustNot.add(notBottleMatchFather);

        JSONArray filter = getFilterCondition(baseDc);
        if (MXJudgeUtils.isNotEmpty(filter)) {
            bool.put("filter", filter);
        }

        bool.put("must", must);
        bool.put("must_not", mustNot);
        query.put("bool", bool);

        return query;
    }

    /**
     * 根据传递过来的条件map拼接召回条件
     */
    JSONObject constructQueryWithCondition(T baseDc, Map<String, Integer> conditionMap) {
        JSONObject query = new JSONObject();
        JSONObject bool = new JSONObject();
        JSONArray must = new JSONArray();
        JSONArray mustNot = new JSONArray();

        if (MXJudgeUtils.isEmpty(conditionMap)) {
            return null;
        }

        conditionMap.forEach((k, v) -> {
            JSONObject specialMatchFather = new JSONObject();
            JSONObject specialMatch = new JSONObject();
            specialMatch.put(k, v);
            specialMatchFather.put("match", specialMatch);
            must.add(specialMatchFather);
        });

        if (!conditionMap.containsKey("status")) {
            JSONObject onlineMatchFather = new JSONObject();
            JSONObject onlineMatch = new JSONObject();
            onlineMatch.put("status", DefineTool.OnlineStatusesEnum.ONLINE.getIndex());
            onlineMatchFather.put("match", onlineMatch);
            must.add(onlineMatchFather);
        }

        if (DefineTool.TabInfoEnum.GIF.getId().equals(baseDc.req.getTabId())) {
            JSONObject rangeFather = new JSONObject();
            JSONObject field = new JSONObject();
            JSONObject range = new JSONObject();
            range.put("gte", DURATION_FILTER_MILLSECONDS);
            field.put("target_gif_info.duration", range);
            rangeFather.put("range", field);
            must.add(rangeFather);
        }

        // 漂流瓶视频不可以召回
        JSONObject notBottleMatchFather = new JSONObject();
        JSONObject notBottleMatch = new JSONObject();
        notBottleMatch.put("is_bottle", 1);
        notBottleMatchFather.put("match", notBottleMatch);
        mustNot.add(notBottleMatchFather);

//        JSONArray filter = getFilterCondition(baseDc);
//        if (!filter.isEmpty()) {
//            bool.put("filter", filter);
//        }

        bool.put("must", must);
        bool.put("must_not", mustNot);
        query.put("bool", bool);

        return query;
    }

    /**
     * 根据传递过来的条件map拼接召回条件
     */
    JSONObject constructQueryByCondition(T baseDc, Map<String, String> conditionMap) {
        JSONObject query = new JSONObject();
        JSONObject bool = new JSONObject();
        JSONArray must = new JSONArray();
        JSONArray mustNot = new JSONArray();

        if (MXJudgeUtils.isEmpty(conditionMap)) {
            return null;
        }

        conditionMap.forEach((k, v) -> {
            JSONObject specialMatchFather = new JSONObject();
            JSONObject specialMatch = new JSONObject();
            specialMatch.put(k, v);
            specialMatchFather.put("match", specialMatch);
            must.add(specialMatchFather);
        });

        if (!conditionMap.containsKey("status")) {
            JSONObject onlineMatchFather = new JSONObject();
            JSONObject onlineMatch = new JSONObject();
            onlineMatch.put("status", DefineTool.OnlineStatusesEnum.ONLINE.getIndex());
            onlineMatchFather.put("match", onlineMatch);
            must.add(onlineMatchFather);
        }

        bool.put("must", must);
        bool.put("must_not", mustNot);
        query.put("bool", bool);

        return query;
    }

    /**
     * 在召回的时候过滤private视频
     */
    JSONObject constructQueryByConditionFilterPrivateVideo(Map<String, String> conditionMap) {
        JSONObject query = new JSONObject();
        JSONObject bool = new JSONObject();
        JSONArray must = new JSONArray();
        JSONArray mustNot = new JSONArray();

        if (MXJudgeUtils.isEmpty(conditionMap)) {
            return null;
        }

        conditionMap.forEach((k, v) -> {
            JSONObject specialMatchFather = new JSONObject();
            JSONObject specialMatch = new JSONObject();
            specialMatch.put(k, v);
            specialMatchFather.put("match", specialMatch);
            must.add(specialMatchFather);
        });

        JSONObject specialMatchFather = new JSONObject();
        JSONObject specialMatch = new JSONObject();
        specialMatch.put("view_privacy", 2);
        specialMatchFather.put("match", specialMatch);

        JSONObject isDeleteFather = new JSONObject();
        JSONObject isDelete = new JSONObject();
        isDelete.put("is_delete", true);
        isDeleteFather.put("match", isDelete);

        mustNot.add(specialMatchFather);
        mustNot.add(isDeleteFather);

        if (!conditionMap.containsKey("status")) {
            JSONObject onlineMatchFather = new JSONObject();
            JSONObject onlineMatch = new JSONObject();
            onlineMatch.put("status", DefineTool.OnlineStatusesEnum.ONLINE.getIndex());
            onlineMatchFather.put("match", onlineMatch);
            must.add(onlineMatchFather);
        }

        bool.put("must", must);
        bool.put("must_not", mustNot);
        query.put("bool", bool);

        return query;
    }

    JSONObject constructQueryByConditionFilterPrivateVideoTerms(String fieldName, List<String> list) {
        JSONObject query = new JSONObject();
        JSONObject bool = new JSONObject();
        JSONArray must = new JSONArray();
        JSONArray mustNot = new JSONArray();

        if (MXJudgeUtils.isEmpty(list)) {
            return null;
        }

        JSONObject existMatchFather = new JSONObject();
        JSONObject existMatch = new JSONObject();
        existMatch.put("field", "is_porn");
        existMatchFather.put("exists", existMatch);
        must.add(existMatchFather);

        JSONObject specialTermsFather = new JSONObject();
        JSONObject specialTerms = new JSONObject();
        JSONArray termsIds = new JSONArray();
        termsIds.addAll(list);
        specialTerms.put(fieldName, list);
        specialTermsFather.put("terms", specialTerms);
        must.add(specialTermsFather);

        JSONObject specialMatchFather = new JSONObject();
        JSONObject specialMatch = new JSONObject();
        specialMatch.put("view_privacy", 2);
        specialMatchFather.put("match", specialMatch);

        JSONObject isDeleteFather = new JSONObject();
        JSONObject isDelete = new JSONObject();
        isDelete.put("is_delete", true);
        isDeleteFather.put("match", isDelete);

        mustNot.add(specialMatchFather);
        mustNot.add(isDeleteFather);

        bool.put("must", must);
        bool.put("must_not", mustNot);
        query.put("bool", bool);

        return query;
    }



    /**
     * 在召回的时候过滤private视频
     */
    @Nullable
    JSONObject constructQueryFilterPorn(Map<String, String> conditionMap) {
        JSONObject query = new JSONObject();
        JSONObject bool = new JSONObject();
        JSONArray must = new JSONArray();
        JSONArray mustNot = new JSONArray();

        if (MXJudgeUtils.isEmpty(conditionMap)) {
            return null;
        }

        conditionMap.forEach((k, v) -> {
            JSONObject specialMatchFather = new JSONObject();
            JSONObject specialMatch = new JSONObject();
            specialMatch.put(k, v);
            specialMatchFather.put("match", specialMatch);
            must.add(specialMatchFather);
        });

        JSONObject existMatchFather = new JSONObject();
        JSONObject existMatch = new JSONObject();
        existMatch.put("field", "is_porn");
        existMatchFather.put("exists", existMatch);
        must.add(existMatchFather);

        JSONObject specialMatchFather = new JSONObject();
        JSONObject specialMatch = new JSONObject();
        specialMatch.put("view_privacy", 2);
        specialMatchFather.put("match", specialMatch);

        JSONObject isDeleteFather = new JSONObject();
        JSONObject isDelete = new JSONObject();
        isDelete.put("is_delete", true);
        isDeleteFather.put("match", isDelete);

        mustNot.add(specialMatchFather);
        mustNot.add(isDeleteFather);

        if (!conditionMap.containsKey("status")) {
            JSONObject onlineMatchFather = new JSONObject();
            JSONObject onlineMatch = new JSONObject();
            onlineMatch.put("status", DefineTool.OnlineStatusesEnum.ONLINE.getIndex());
            onlineMatchFather.put("match", onlineMatch);
            must.add(onlineMatchFather);
        }

        bool.put("must", must);
        bool.put("must_not", mustNot);
        query.put("bool", bool);

        return query;
    }

    /**
     * 根据传递过来的条件map和_id must_not信息，拼接召回条件
     */
    JSONObject constructQueryByCondition(T baseDc, Map<String, String> conditionMap, List<String> mustNotIdList) {
        JSONObject query = new JSONObject();
        JSONObject bool = new JSONObject();
        JSONArray must = new JSONArray();
        JSONArray mustNot = new JSONArray();

        if (MXJudgeUtils.isEmpty(conditionMap)) {
            return null;
        }

        conditionMap.forEach((k, v) -> {
            JSONObject specialMatchFather = new JSONObject();
            JSONObject specialMatch = new JSONObject();
            specialMatch.put(k, v);
            specialMatchFather.put("match", specialMatch);
            must.add(specialMatchFather);
        });

        mustNotIdList.forEach((v) -> {
            JSONObject specialMatchFather = new JSONObject();
            JSONObject specialMatch = new JSONObject();
            specialMatch.put("_id", v);
            specialMatchFather.put("match", specialMatch);
            mustNot.add(specialMatchFather);
        });

        if (!conditionMap.containsKey("status")) {
            JSONObject onlineMatchFather = new JSONObject();
            JSONObject onlineMatch = new JSONObject();
            onlineMatch.put("status", DefineTool.OnlineStatusesEnum.ONLINE.getIndex());
            onlineMatchFather.put("match", onlineMatch);
            must.add(onlineMatchFather);
        }

        bool.put("must", must);
        bool.put("must_not", mustNot);
        query.put("bool", bool);

        return query;
    }

    /**
     * 根据传递过来的条件map拼接召回条件
     */
    JSONObject constructTerms(String matchKey, List<String> idList) {
        JSONObject query = new JSONObject();
        JSONObject bool = new JSONObject();
        JSONArray must = new JSONArray();
        JSONArray mustNot = new JSONArray();

        JSONObject termsFather = new JSONObject();
        JSONObject terms = new JSONObject();
        terms.put(matchKey, idList);
        termsFather.put("terms", terms);
        must.add(termsFather);

        JSONObject onlineMatchFather = new JSONObject();
        JSONObject onlineMatch = new JSONObject();
        onlineMatch.put("status", DefineTool.OnlineStatusesEnum.ONLINE.getIndex());
        onlineMatchFather.put("match", onlineMatch);
        must.add(onlineMatchFather);

        bool.put("must", must);
        bool.put("must_not", mustNot);
        query.put("bool", bool);

        return query;
    }

    /**
     * 根据传递过来的条件map拼接召回条件
     */
    JSONObject constructQueryWithMatch(T baseDc, String matchCondition) {
        JSONObject query = new JSONObject();
        JSONObject bool = new JSONObject();
        JSONArray must = new JSONArray();
        JSONArray mustNot = new JSONArray();

        if (MXJudgeUtils.isEmpty(matchCondition)) {
            return null;
        }

        if ("tiktok_special".equals(matchCondition)) {
            JSONObject matchFather = new JSONObject();
            JSONObject match = new JSONObject();
            match.put("app_name.keyword", "tiktok");
            matchFather.put("match", match);
            must.add(matchFather);

            JSONObject specialMatchFather = new JSONObject();
            JSONObject specialMatch = new JSONObject();
            specialMatch.put("tiktok_sign", 1);
            specialMatchFather.put("match", specialMatch);
            must.add(specialMatchFather);
        } else if ("tiktok".equals(matchCondition)) {
            JSONObject matchFather = new JSONObject();
            JSONObject match = new JSONObject();
            match.put("app_name.keyword", "tiktok");
            matchFather.put("match", match);
            must.add(matchFather);

            JSONObject specialMatchFather = new JSONObject();
            JSONObject specialMatch = new JSONObject();
            specialMatch.put("tiktok_sign", 1);
            specialMatchFather.put("match", specialMatch);
            mustNot.add(specialMatchFather);

            JSONObject rangeFather = new JSONObject();
            JSONObject range = new JSONObject();
            JSONObject rangeChild = new JSONObject();
            rangeChild.put("gte", 60);
            range.put("comment_count", rangeChild);
            rangeFather.put("range", range);
            must.add(rangeFather);
        } else {
            JSONObject specialMatchFather = new JSONObject();
            JSONObject specialMatch = new JSONObject();
            specialMatch.put("app_name.keyword", matchCondition);
            specialMatchFather.put("match", specialMatch);
            must.add(specialMatchFather);

        }

        // 漂流瓶视频不可以召回
        JSONObject notBottleMatchFather = new JSONObject();
        JSONObject notBottleMatch = new JSONObject();
        notBottleMatch.put("is_bottle", 1);
        notBottleMatchFather.put("match", notBottleMatch);
        mustNot.add(notBottleMatchFather);

        JSONObject onlineMatchFather = new JSONObject();
        JSONObject onlineMatch = new JSONObject();
        onlineMatch.put("status", DefineTool.OnlineStatusesEnum.ONLINE.getIndex());
        onlineMatchFather.put("match", onlineMatch);
        must.add(onlineMatchFather);

        if (DefineTool.TabInfoEnum.GIF.getId().equals(baseDc.req.getTabId())) {
            JSONObject rangeFather = new JSONObject();
            JSONObject field = new JSONObject();
            JSONObject range = new JSONObject();
            range.put("gte", DURATION_FILTER_MILLSECONDS);
            field.put("target_gif_info.duration", range);
            rangeFather.put("range", field);
            must.add(rangeFather);
        }

        JSONArray filter = getFilterCondition(baseDc);
        if (MXJudgeUtils.isNotEmpty(filter)) {
            bool.put("filter", filter);
        }

        bool.put("must", must);
        bool.put("mustNot", mustNot);
        query.put("bool", bool);

        return query;
    }

    /**
     * 构造按照heat相应类别的请求query
     */
    JSONObject constructQueryHeat(T baseDc) {
        JSONObject query = new JSONObject();
        JSONObject bool = new JSONObject();
        JSONArray must = new JSONArray();
        JSONArray mustNot = new JSONArray();

        JSONObject matchFather = new JSONObject();
        JSONObject match = new JSONObject();
        match.put("status", DefineTool.OnlineStatusesEnum.ONLINE.getIndex());
        matchFather.put("match", match);
        must.add(matchFather);

        JSONArray filter = getFilterCondition(baseDc);
        if (MXJudgeUtils.isNotEmpty(filter)) {
            bool.put("filter", filter);
        }

        // 漂流瓶视频不可以召回
        JSONObject notBottleMatchFather = new JSONObject();
        JSONObject notBottleMatch = new JSONObject();
        notBottleMatch.put("is_bottle", 1);
        notBottleMatchFather.put("match", notBottleMatch);
        mustNot.add(notBottleMatchFather);

        JSONObject specialNotLikeMatchFather = new JSONObject();
        JSONObject speciaNotLikelMatch = new JSONObject();
        speciaNotLikelMatch.put("special_sign", -1);
        specialNotLikeMatchFather.put("match", speciaNotLikelMatch);
        mustNot.add(specialNotLikeMatchFather);

        bool.put("must", must);
        bool.put("must_not", mustNot);
        query.put("bool", bool);

        return query;
    }

    /**
     * 构造上传的或者标记为好的视频中没有被click过的
     */
    JSONObject constructQueryNoClickGoodVideo(T baseDc) {
        JSONObject query = new JSONObject();
        JSONObject bool = new JSONObject();
        JSONArray must = new JSONArray();
        JSONArray mustNot = new JSONArray();
        JSONArray should = new JSONArray();

        JSONObject matchFather = new JSONObject();
        JSONObject match = new JSONObject();
        match.put("special_sign", 1);
        matchFather.put("match", match);
        should.add(matchFather);

        JSONObject specialMatchFather = new JSONObject();
        JSONObject specialMatch = new JSONObject();
        specialMatch.put("upload_sign", 1);
        specialMatchFather.put("match", specialMatch);
        should.add(specialMatchFather);

        JSONObject onlineMatchFather = new JSONObject();
        JSONObject onlineMatch = new JSONObject();
        onlineMatch.put("status", DefineTool.OnlineStatusesEnum.ONLINE.getIndex());
        onlineMatchFather.put("match", onlineMatch);
        must.add(onlineMatchFather);

        // 漂流瓶视频不可以召回
        JSONObject notBottleMatchFather = new JSONObject();
        JSONObject notBottleMatch = new JSONObject();
        notBottleMatch.put("is_bottle", 1);
        notBottleMatchFather.put("match", notBottleMatch);
        mustNot.add(notBottleMatchFather);

        JSONArray filter = getFilterCondition(baseDc);
        if (MXJudgeUtils.isNotEmpty(filter)) {
            bool.put("filter", filter);
        }

        bool.put("must", must);
        bool.put("should", should);
        bool.put("must_not", mustNot);
        bool.put("minimum_should_match", 1);

        query.put("bool", bool);

        return query;
    }

    /**
     * 在线计算高宽比
     */
    public JSONArray getFilterCondition(T baseDc) {
        JSONArray filter = new JSONArray();
        JSONObject scriptFather = new JSONObject();
        JSONObject scriptOne = new JSONObject();
        JSONObject source = new JSONObject();
        if (DefineTool.TabInfoEnum.HOT.getId().equals(baseDc.req.getTabId())) {
            source.put("source", "doc['thumbnail_info.height'].value - (0.9 * doc['thumbnail_info.width'].value) >= 0");
        } else if (DefineTool.TabInfoEnum.STATUS.getId().equals(baseDc.req.getTabId())) {
            source.put("source", "doc['thumbnail_info.height'].value - (0.9 * doc['thumbnail_info.width'].value) < 0");
        }

        if (MXJudgeUtils.isNotEmpty(source)) {
            source.put("lang", "painless");
            scriptOne.put("script", source);
            scriptFather.put("script", scriptOne);
            filter.add(scriptFather);
        }

        return filter;
    }

    /**
     * 构造相应类别的请求content
     */
    public JSONObject constructContent(JSONObject query, int from, int size, JSONArray source, JSONArray sort) {
        JSONObject content = new JSONObject();
        if (query != null) {
            content.put("query", query);
        }
        content.put("size", size);
        if (MXJudgeUtils.isNotEmpty(source)) {
            content.put("_source", source);
        }
        if (MXJudgeUtils.isNotEmpty(sort)) {
            content.put("sort", sort);
        }

        return content;
    }

    public JSONObject constructContentWithNextToken(JSONObject query, int size, JSONArray source, JSONArray sort, JSONArray nextToken) {
        JSONObject content = new JSONObject();
        if (query != null) {
            content.put("query", query);
        }
        content.put("size", size);
        if (MXJudgeUtils.isNotEmpty(source)) {
            content.put("_source", source);
        }
        if (MXJudgeUtils.isNotEmpty(sort)) {
            content.put("sort", sort);
        }
        if (MXJudgeUtils.isNotEmpty(nextToken)) {
            content.put("search_after", nextToken);
        }

        return content;
    }

    public JSONObject constructContentWithTotalNum(JSONObject query, int size, JSONArray source, JSONArray sort) {
        JSONObject content = new JSONObject();
        if (query != null) {
            content.put("query", query);
        }
        content.put("size", size);
        if (MXJudgeUtils.isNotEmpty(source)) {
            content.put("_source", source);
        }
        if (MXJudgeUtils.isNotEmpty(sort)) {
            content.put("sort", sort);
        }

        content.put("track_total_hits", true);

        return content;
    }

    void sort(Map<String, StrategyPoolConf> temp, Map<String, StrategyPoolConf> map) {
        List<Map.Entry<String, StrategyPoolConf>> list = new ArrayList<>(temp.entrySet());
        list.sort((o1, o2) -> o2.getValue().priority - o1.getValue().priority);
        for (Map.Entry<String, StrategyPoolConf> entry : list) {
            map.put(entry.getKey(), entry.getValue());
        }
    }
}
