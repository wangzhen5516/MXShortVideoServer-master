package mx.j2.recommend.recall.impl;

import com.alibaba.fastjson.JSONObject;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.data_collection.InternalDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_source.ElasticSearchDataSource;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXJudgeUtils;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author zhiyuan.wang
 * @date 2021/7/21 11:16 上午
 */
public class InternalVideosOfTagRecall extends InternalBaseRecall {
    private static final String FIELD_KEY = "_id";
    private static final int RECALL_SIZE = 1;
    String requestUrlFormat = "/%s/_search?pretty=false";
    private static final String INDEX_URL = DefineTool.CategoryEnum.HASHTAG_INFO.getIndexAndType();


    private final Set<String> iplHashTag = new HashSet<>(
            Arrays.asList("kheltakatak",
                    "fandancemove",
                    "batbalance",
                    "bolcricket",
                    "chakhdephatte",
                    "laphaokkr",
                    "turoarmacha",
                    "mipaltandance",
                    "hallabol",
                    "goorangearmy"
            )
    );

    // TODO 投资方运营tag，豁免音乐过滤
    private final String PEPSI_TAG = "";

    @Override
    public void recall(InternalDataCollection dc) {
        List<String> resourceIdList = dc.internalReq.resourceIdList;
        String lowerTag = resourceIdList.get(0).toLowerCase();
        int from = Integer.parseInt(resourceIdList.get(1));
        int size = Integer.parseInt(resourceIdList.get(2));

        List<String> videoIds = getCmsTop(dc, lowerTag);
        List<String> result = new ArrayList<>();
        if (MXJudgeUtils.isNotEmpty(videoIds)) {
            dc.totalNumber = videoIds.size();
            if (from <= videoIds.size() && (from + size) <= videoIds.size()) {
                // 结果全部都在cms的数据中
                result = videoIds.subList(from, from + size);
            } else if (from <= videoIds.size()) {
                // 结果部分在cms的数据中
                result = videoIds.subList(from, videoIds.size());
            }

            if (MXJudgeUtils.isNotEmpty(result)) {
                //  部分视频Id和视频总数需要从es中获取
                from = 0;
                size = size - result.size();
            } else {
                // 视频Id和视频总数都需要从es中获取
                from = from - videoIds.size();
            }
        }
        List<BaseDocument> resultDocumentList = MXDataSource.details().get(result, getName());
        dc.mergedList.addAll(resultDocumentList);
        String esContent = constructQuery(lowerTag, videoIds, from, size);

        String elasticSearchRequest = String.format(requestUrlFormat, DefineTool.CategoryEnum.SHORT_VIDEO.getIndexAndType());

        dc.addToESRequestList(
                elasticSearchRequest,
                esContent,
                this.getName(), "",
                DefineTool.EsType.VIDEO.getTypeName()
        );

        dc.resultFromMap.put(this.getName(), DefineTool.RecallFrom.ES.getName());
        dc.searchEngineRecallerSet.add(this.getName());
    }

    private List<String> getCmsTop(InternalDataCollection dc, String lowerTag) {
        String elasticSearchRequest = String.format(requestUrlFormat, INDEX_URL);
        List<JSONObject> tagDetailResult = assembleRequest(dc, elasticSearchRequest, lowerTag);
        return extractIds(tagDetailResult);
    }

    private String constructQuery(String lowerTag, List<String> videoIds, int from, int size) {
        String esContent;
        String HEAD_ES_CONTENT_FORMAT;

        if (iplHashTag.contains(lowerTag)) {
            HEAD_ES_CONTENT_FORMAT = "{\"from\":%s,\"size\":%s,\"query\":{\"bool\":{\"must_not\":[%s {\"nested\":{\"path\":\"hashtag_match_info_es\",\"query\":{\"bool\":{\"filter\":{\"range\":{\"hashtag_match_info_es.score\":{\"lte\":6}},\"must\":[{\"match\":{\"hashtag_match_info_es.hash\":\"%s\"}}]}}}}},{\"match\":{\"view_privacy\":\"2\"}},{\"match\":{\"is_delete\":\"true\"}},{\"match\":{\"countries\":\"BGD\"}},{\"match\":{\"countries\":\"PAK\"}},{\"match\":{\"private_count\":1}},{\"match\":{\"is_delogo\":true}},{\"match\":{\"is_duplicated\":true}},{\"match\":{\"is_ipl\":true}}],\"must\":[{\"match\":{\"desc_tag\":\"%s\"}},{\"match\":{\"status\":\"1\"}},{\"exists\":{\"field\":\"is_porn\"}},{\"match\":{\"human_reviewed_status\":\"1\"}}]}},\"sort\":[{\"hashtag_heat\":{\"order\":\"desc\",\"missing\":0}},{\"online_time\":{\"order\":\"desc\"}}]}";
        } else if (PEPSI_TAG.equals(lowerTag)) {
            HEAD_ES_CONTENT_FORMAT = "{\"from\":%s,\"size\":%s,\"query\":{\"bool\":{\"must_not\":[%s {\"match\":{\"view_privacy\":\"2\"}},{\"match\":{\"is_delete\":\"true\"}},{\"match\":{\"countries\":\"BGD\"}},{\"match\":{\"countries\":\"PAK\"}},{\"match\":{\"private_count\":1}},{\"match\":{\"is_delogo\":true}},{\"match\":{\"is_duplicated\":true}},{\"match\":{\"is_ipl\":true}}], \"must\":[{\"match\":{\"desc_tag\":\"%s\"}},{\"match\":{\"status\":\"1\"}},{\"exists\":{\"field\":\"is_porn\"}}]}},\"sort\":[{\"hashtag_heat\":{\"order\":\"desc\",\"missing\":0}},{\"online_time\":{\"order\":\"desc\"}}]}";
        } else {
            HEAD_ES_CONTENT_FORMAT = "{\"from\":%s,\"size\":%s,\"query\":{\"bool\":{\"must_not\":[%s {\"nested\":{\"path\":\"hashtag_match_info_es\",\"query\":{\"bool\":{\"filter\":{\"range\":{\"hashtag_match_info_es.score\":{\"lte\":6}}},\"must\":[{\"match\":{\"hashtag_match_info_es.hash\":\"%s\"}}]}}}},{\"match\":{\"view_privacy\":\"2\"}},{\"match\":{\"is_delete\":\"true\"}},{\"match\":{\"countries\":\"BGD\"}},{\"match\":{\"countries\":\"PAK\"}},{\"match\":{\"private_count\":1}},{\"match\":{\"is_delogo\":true}},{\"match\":{\"is_duplicated\":true}},{\"match\":{\"is_ipl\":true}}],\"must\":[{\"match\":{\"desc_tag\":\"%s\"}},{\"match\":{\"status\":\"1\"}},{\"exists\":{\"field\":\"is_porn\"}}]}},\"sort\":[{\"hashtag_heat\":{\"order\":\"desc\",\"missing\":0}},{\"online_time\":{\"order\":\"desc\"}}]}";
        }

        String terms = "";
        if (MXJudgeUtils.isNotEmpty(videoIds)) {
            // 构架es过滤语句，将cms设置的视频从 es查询中过滤掉
            String ids = videoIds.stream().map(x -> "\"" + x + "\"").collect(Collectors.joining(","));
            StringBuilder sb = new StringBuilder();
            terms = sb.append("{\"terms\":{\"_id\":[").append(ids).append("]}},").toString();
        }

        if (PEPSI_TAG.equals(lowerTag)) {
            esContent = String.format(HEAD_ES_CONTENT_FORMAT, from, size, terms, lowerTag);
        } else {
            esContent = String.format(HEAD_ES_CONTENT_FORMAT, from, size, terms, lowerTag, lowerTag);
        }
        return esContent;
    }

    private List<JSONObject> assembleRequest(BaseDataCollection dc, String elasticSearchRequest, String value) {
        String request = constructQueryByConditionForHashTag(value).toJSONString();

        dc.resultFromMap.put(this.getName(), DefineTool.RecallFrom.ES.getName());
        // 拿到 hashTag 的详情
        ElasticSearchDataSource elasticSearchDataSource = MXDataSource.ES();
        return elasticSearchDataSource.sendSyncSearchPure(elasticSearchRequest, request);
    }

    /**
     * 为Hashtag类型的搜索拼接query语句
     */
    private JSONObject constructQueryByConditionForHashTag(String value) {
        JSONObject field = new JSONObject();
        JSONObject match = new JSONObject();
        field.put(FIELD_KEY, value);
        match.put("match", field);

        JSONObject content = new JSONObject();
        content.put("query", match);
        content.put("size", RECALL_SIZE);

        return content;
    }

    private List<String> extractIds(List<JSONObject> tagDetailResult) {
        if (MXJudgeUtils.isEmpty(tagDetailResult) || tagDetailResult.get(0).isEmpty() || !tagDetailResult.get(0).containsKey("video_ids")) {
            return null;
        }
        return tagDetailResult.get(0).getJSONArray("video_ids").toJavaList(String.class);
    }
}
