package mx.j2.recommend.recall.impl;

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

import java.util.List;

/**
 * @author DuoZhao
 * @ Author     ：DuoZhao
 * @ Date       ：Created in 下午8:42 2020/09/08
 * @ Description：${description}
 */

public class VideosOfTheStickOnTopRecall extends BaseRecall<BaseDataCollection> {

    private static Logger log = LogManager.getLogger(VideosOfTheStickOnTopRecall.class);

    private static final String FIELD_KEY = "_id";
    private static final int RECALL_SIZE = 1;
    private static final String INDEX_URL = DefineTool.CategoryEnum.HASHTAG_INFO.getIndexAndType();

    /**
     * 构造函数
     */
    public VideosOfTheStickOnTopRecall() {
    }

    @Override
    public boolean skip(BaseDataCollection dc) {
        if (MXJudgeUtils.isEmpty(dc.req.resourceType) || MXJudgeUtils.isEmpty(dc.req.resourceId)
                || !"tag".equals(dc.req.resourceType)) {
            return true;
        }
        return false;
    }

    @Override
    @Trace(dispatcher = true)
    public void recall(BaseDataCollection dc) {
        LocalCacheDataSource dataSource = MXDataSource.cache();
        String localCache = String.format("%s_%s", dc.req.resourceId, this.getName());
        List<JSONObject> tagDetailResult = dataSource.getVideosStickOnTopCache(localCache);

        // 获取 Hashtag 的详情
        if(MXJudgeUtils.isEmpty(tagDetailResult)) {
            String elasticSearchRequest = String.format(requestUrlFormat, INDEX_URL);

            tagDetailResult = assembleRequest(dc, elasticSearchRequest, dc.req.getResourceId().toLowerCase());
            dataSource.setVideosStickOnTopCache(localCache, tagDetailResult);
        }

        // 获取置顶video的id和详情
        List<String> videoIds = extractIds(tagDetailResult);
        List<BaseDocument> resultDocumentList = MXDataSource.details().get(videoIds, getName());

        dc.hashTagTopVideoList.addAll(resultDocumentList);
    }

    private List<JSONObject> assembleRequest(BaseDataCollection dc, String elasticSearchRequest, String value) {
        String request = constructQueryByConditionForHashTag(value).toJSONString();

        if (log.isDebugEnabled()) {
            log.debug(String.format("VideoOfTheStickOnTopRecall search url : %s", request));
            log.debug(String.format("VideoOfTheStickOnTopRecall search url : %s", elasticSearchRequest));
        }

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
