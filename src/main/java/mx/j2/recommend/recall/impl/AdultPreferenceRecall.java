package mx.j2.recommend.recall.impl;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import mx.j2.recommend.component.configurable.config.RecallConfig;
import mx.j2.recommend.data_model.UserProfile;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_source.LocalCacheDataSource;
import mx.j2.recommend.data_source.UserStrategyTagDataSource;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.MXJudgeUtils;

import java.util.List;
import java.util.Map;

public class AdultPreferenceRecall extends BaseRecall<BaseDataCollection> {
    private final String ES_INDEX = "index";
    private final String LOCAL_CACHE_KEY = "localCacheKey";
    private final String ADULT_TAG_USER_TABLE = "table";
    private final String ADULT_TAG_NAME = "gongzuo";
    private final String NONE_TABLE_SIGNAL = "none";

    private final String REQUEST_URL_FORMAT = "/%s/_search?pretty=false";
    private final int RECALL_SIZE = 200;
    private static JSONArray SORT_JSON;

    static {
        SORT_JSON = new JSONArray();
        JSONObject sortCore = new JSONObject();
        JSONObject script = new JSONObject();
        script.put("script", "Math.random()");
        script.put("type", "number");
        script.put("order", "asc");
        sortCore.put("_script", script);
        SORT_JSON.add(sortCore);
    }

    @Override
    public void registerConfig(Map<String, Class> outConfMap) {
        outConfMap.put(RecallConfig.KEY_RESULT, String.class);
        outConfMap.put(ES_INDEX, String.class);
        outConfMap.put(LOCAL_CACHE_KEY, String.class);
        outConfMap.put(ADULT_TAG_USER_TABLE, String.class);
    }

    @Override
    public void recall(BaseDataCollection dc) {
        LocalCacheDataSource localCacheDataSource = MXDataSource.cache();
        String localCacheKey = config.getString(LOCAL_CACHE_KEY);
        List<BaseDocument> docList = localCacheDataSource.getAdultPreferenceVideoListCache(localCacheKey);

        String esIndex = config.getString(ES_INDEX);
        if (MXJudgeUtils.isEmpty(docList)) {
            BaseDataCollection.ESRequest esRequest = constructRequest(esIndex);
            docList = MXDataSource.ES().searchForDocuments(esRequest);
            if (MXJudgeUtils.isEmpty(docList)) {
                return;
            }
        }

        String adultUserTable = config.getString(ADULT_TAG_USER_TABLE);
        if (!NONE_TABLE_SIGNAL.equals(adultUserTable)) {
            checkAdultPreferenceTag(dc, adultUserTable);
        }

        dc.adultPreferenceDocumentList.addAll(docList);
        dc.syncSearchResultSizeMap.put(this.getName(), docList.size());
    }

    private void checkAdultPreferenceTag(BaseDataCollection dc, String tableName) {
        UserStrategyTagDataSource dataSource = MXDataSource.profileTagV2();
        dc.tagTableName = tableName;
        List<UserProfile.Tag> tags = dataSource.getTags(dc);
        if (MXJudgeUtils.isEmpty(tags)) {
            return;
        }
        dc.isAdultUuid = tags.stream().anyMatch(tag -> ADULT_TAG_NAME.equals(tag.name));
    }

    private BaseDataCollection.ESRequest constructRequest(String index) {
        String elasticSearchRequest = String.format(REQUEST_URL_FORMAT, index);
        String content = constructContent(null, 0, RECALL_SIZE, null, SORT_JSON).toString();
        return new BaseDataCollection.ESRequest(elasticSearchRequest, content, this.getName(), "", "pool");
    }
}
