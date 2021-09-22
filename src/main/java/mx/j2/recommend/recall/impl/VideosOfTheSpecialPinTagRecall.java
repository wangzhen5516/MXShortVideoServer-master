package mx.j2.recommend.recall.impl;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.newrelic.api.agent.Trace;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_source.ElasticSearchDataSource;
import mx.j2.recommend.data_source.LocalCacheDataSource;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXJudgeUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author DuoZhao
 * @ Author     ：DuoZhao
 * @ Date       ：Created in 2020/12/25
 */
public class VideosOfTheSpecialPinTagRecall extends BaseRecall<BaseDataCollection> {

    private static Logger log = LogManager.getLogger(VideosOfTheSpecialPinTagRecall.class);
    private static final String indexUrl = DefineTool.CategoryEnum.HASHTAG_INFO.getIndexAndType();

    /**
     * 构造函数
     */
    public VideosOfTheSpecialPinTagRecall() {
    }

    @Override
    public boolean skip(BaseDataCollection dc) {
        if (MXJudgeUtils.isEmpty(dc.req.resourceType) || MXJudgeUtils.isEmpty(dc.req.resourceId) ||
                !"pintag".equals(dc.req.resourceType)) {
            return true;
        }
        return false;
    }

    @Override
    @Trace(dispatcher = true)
    public void recall(BaseDataCollection dc) {
        LocalCacheDataSource dataSource = MXDataSource.cache();
        String localCache = String.format("%s_%s", dc.req.resourceId, this.getName());
        List<String> videoIdList = dataSource.getSpecialPinTagVideoListCache(localCache);

        if (MXJudgeUtils.isEmpty(videoIdList)) {
            String elasticSearchRequest = String.format(requestUrlFormat, indexUrl);
            videoIdList = new ArrayList<>();

            Map<String, String> condition = new HashMap<>();
            condition.put("_id", dc.req.getResourceId().toLowerCase());
            List<JSONObject> tagDetailResult = assembleRequest(dc, elasticSearchRequest, condition);

            // 分离出 video_ids
            if (MXJudgeUtils.isEmpty(tagDetailResult) || tagDetailResult.get(0).isEmpty() || !tagDetailResult.get(0).containsKey("video_ids")) {
                return;
            }
            JSONArray ids = tagDetailResult.get(0).getJSONArray("video_ids");
            for (int i = 0; i < ids.size(); i++) {
                videoIdList.add(ids.getString(i));
            }
            dataSource.setSpecialPinTagVideoListCache(localCache, videoIdList);
        }

        // 提前对所需内容进行切割，以免过度使用CA拉去详情
        int cutNumStart;
        int cutNumEnd;
        if (MXJudgeUtils.isEmpty(dc.req.nextToken)) {
            cutNumStart = 0;
            cutNumEnd = Math.min((dc.req.num + 10), videoIdList.size());
        } else {
            cutNumStart = videoIdList.indexOf(dc.req.nextToken) + 1;
            cutNumEnd = Math.min((cutNumStart + dc.req.num + 10), videoIdList.size());
        }

        // 拉取 video_ids 的详情
        List<BaseDocument> resultDocumentList = MXDataSource.details().get(videoIdList.subList(cutNumStart, cutNumEnd), getName());
        for (BaseDocument doc : resultDocumentList) {
            doc.nextTokenMap.put(dc.req.interfaceName, doc.id);
        }

        dc.mergedList.addAll(resultDocumentList);
    }

    private List<JSONObject> assembleRequest(BaseDataCollection dc, String elasticSearchRequest, Map<String, String> conditionMap) {
        JSONObject query = constructQueryByConditionForHashTag(dc, conditionMap);
        if (MXJudgeUtils.isEmpty(query)) {
            return null;
        }

        JSONObject content = constructContent(query, 0, 1, null, null);
        String request = content.toJSONString();

        if (log.isDebugEnabled()) {
            log.debug(String.format("VideosOfTheSpecialPinTagRecall search url : %s", request));
            log.debug(String.format("VideosOfTheSpecialPinTagRecall search url : %s", elasticSearchRequest));
        }

        dc.resultFromMap.put(this.getName(), DefineTool.RecallFrom.ES.getName());
        // 拿到 hashTag 的详情
        ElasticSearchDataSource elasticSearchDataSource = MXDataSource.ES();
        return elasticSearchDataSource.sendSyncSearchPure(elasticSearchRequest, request);
    }

    /**
     * 为Hashtag类型的搜索拼接query语句
     */
    JSONObject constructQueryByConditionForHashTag(BaseDataCollection baseDc, Map<String, String> conditionMap) {
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
        mustNot.add(specialMatchFather);

        bool.put("must", must);
        bool.put("must_not", mustNot);
        query.put("bool", bool);

        return query;
    }
}

