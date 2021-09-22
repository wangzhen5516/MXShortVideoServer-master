package mx.j2.recommend.recall.impl;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_source.LocalCacheDataSource;
import mx.j2.recommend.data_source.UserProfileDataSource;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.BloomUtil;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXJudgeUtils;
import mx.j2.recommend.util.MXStringUtils;

import java.util.List;
@Deprecated
public class VideosOfPreferredPublisherRecall extends BaseRecall<BaseDataCollection> {

    private static final int CACHE_TIME_SECONDS = 300;
    private static final int RECALL_SIZE = 200;
    private static final String REQUEST_URL_FORMAT = "/%s/_search?pretty=false";
    private static final JSONArray SORT_JSON;
    private static final int LIST_LIMITATION = 100;

    static {
        SORT_JSON = new JSONArray();
        JSONObject sortCore = new JSONObject();
        JSONObject script = new JSONObject();
        script.put("order", "desc");
        sortCore.put("heat_score", script);
        SORT_JSON.add(sortCore);
    }

    @Override
    public boolean skip(BaseDataCollection data) {
        return false;
    }

    @Override
    public void recall(BaseDataCollection baseDc) {
        if (!DefineTool.TabInfoEnum.HOT.getId().equals(baseDc.req.tabId)) {
            return;
        }

        String cacheKey = constructCacheKey(baseDc);

        LocalCacheDataSource localCacheDataSource = MXDataSource.cache();
        List<BaseDocument> cacheDocumentList = localCacheDataSource.getScoreWeightRecallCache(cacheKey);
        //如果本地缓存有数据，直接取出返回
        if (MXJudgeUtils.isNotEmpty(cacheDocumentList)) {
            baseDc.preferredPublisherVideoList.addAll(cacheDocumentList);
            baseDc.resultFromMap.put(this.getName(), DefineTool.RecallFrom.LOCAL.getName());
            baseDc.syncSearchResultSizeMap.put(this.getName(), cacheDocumentList.size());
            return;
        }

        UserProfileDataSource userProfileDataSource = MXDataSource.profile();
        String userProfile = userProfileDataSource.getUserProfileByUuId(BloomUtil.getUuid(baseDc));
        if (MXStringUtils.isEmpty(userProfile)) {
            return;
        }
        List<String> publisherIdList = userProfileDataSource.getUserPreferredPublisherList(userProfile);
        if (MXJudgeUtils.isEmpty(publisherIdList)) {
            return;
        }

        if (publisherIdList.size() > LIST_LIMITATION) {
            publisherIdList = publisherIdList.subList(0, LIST_LIMITATION);
        }

        BaseDataCollection.ESRequest esRequest = constructRequest(publisherIdList);
        List<BaseDocument> docList = MXDataSource.videoES().searchForDocuments(esRequest);
        if (MXJudgeUtils.isEmpty(docList)) {
            return;
        }
        MXDataSource.cache().setScoreWeightRecallCache(cacheKey, docList, CACHE_TIME_SECONDS);
        baseDc.preferredPublisherVideoList.addAll(docList);
        baseDc.resultFromMap.put(this.getName(), DefineTool.RecallFrom.ES.getName());
        baseDc.syncSearchResultSizeMap.put(this.getName(), docList.size());
    }

    private BaseDataCollection.ESRequest constructRequest(List<String> publisherIdList) {
        JSONObject content = new JSONObject();
        JSONObject queryBody = new JSONObject();
        JSONObject termsBody = new JSONObject();
        termsBody.put("publisher_id", publisherIdList);
        queryBody.put("terms", termsBody);
        content.put("query", queryBody);
        content.put("size", RECALL_SIZE);
        content.put("sort", SORT_JSON);
        String elasticSearchRequest = String.format(REQUEST_URL_FORMAT, DefineTool.CategoryEnum.SHORT_VIDEO.getIndexAndType());
        return new BaseDataCollection.ESRequest(elasticSearchRequest, content.toString(), this.getName(), "", "video");
    }

    private String constructCacheKey(BaseDataCollection baseDc) {
        return String.format("%s_%s_%s", this.getName(), baseDc.req.getInterfaceName(), BloomUtil.getUuid(baseDc));
    }
}
