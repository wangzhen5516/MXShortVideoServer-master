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
import mx.j2.recommend.util.MXStringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author ：Qi Mao
 * @date ：Created at 20:00 12/14/2020
 * description: 根据用户小语言列表，从ES/Cache召回视频
 */
@Deprecated
public class MinorLanguageSoundOutRecall extends BaseRecall<BaseDataCollection> {

    private static final int RECALL_SIZE = 200;
    private static final int CACHE_TIME_SECONDS = 300;
    private static final String REQUEST_URL_FORMAT = "/%s/_search?pretty=false";
    private static final String INDEX_URL = "taka_flowpool_special_lang_tags";
    private static final JSONArray SORT_JSON;
    private static final List<String> MINOR_LANGUAGE_LIST = new ArrayList<>();

    static {
        //大语言名单
        MINOR_LANGUAGE_LIST.add("Malayalam");
        MINOR_LANGUAGE_LIST.add("Bengali");
        MINOR_LANGUAGE_LIST.add("Kannada");
        MINOR_LANGUAGE_LIST.add("Punjabi");
        MINOR_LANGUAGE_LIST.add("Telugu");
        MINOR_LANGUAGE_LIST.add("Tamil");
        MINOR_LANGUAGE_LIST.add("Gujarati");
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
    public boolean skip(BaseDataCollection data) {
        return false;
    }

    @Override
    @Trace(dispatcher = true)
    public void recall(BaseDataCollection baseDc) {
        if (!DefineTool.TabInfoEnum.HOT.getId().equals(baseDc.req.tabId)) {
            return;
        }
        List<String> languateList = baseDc.req.languageList;
        List<String> minorLanguageList = findMinor(languateList);

        if (MXJudgeUtils.isEmpty(minorLanguageList)) {
            return;
        }

        String cacheKey = constructCacheKey(minorLanguageList);
        LocalCacheDataSource localCacheDataSource = MXDataSource.cache();
        List<BaseDocument> cacheDocumentList = localCacheDataSource.getScoreWeightRecallCache(cacheKey);

        //如果缓存拉到数据
        if (MXJudgeUtils.isNotEmpty(cacheDocumentList)) {
            baseDc.minorLanguageRecallList.addAll(cacheDocumentList);
            baseDc.resultFromMap.put(this.getName(), DefineTool.RecallFrom.LOCAL.getName());
            baseDc.syncSearchResultSizeMap.put(this.getName(), cacheDocumentList.size());
            return;
        }
        //如果没拉到
        JSONObject queryBody = constructQuery(minorLanguageList);

        if (queryBody == null) {
            return;
        }

        String content = constructContent(queryBody, 0, RECALL_SIZE, null, SORT_JSON).toJSONString();
        String EsRequest = String.format(REQUEST_URL_FORMAT, INDEX_URL);
        BaseDataCollection.ESRequest request = new BaseDataCollection.ESRequest(EsRequest, content, this.getName(), "", "");
        ElasticSearchDataSource elasticSearchDataSource = MXDataSource.ES();
        List<BaseDocument> resultDocumentList = elasticSearchDataSource.searchForDocuments(request);

        if (MXJudgeUtils.isEmpty(resultDocumentList)) {
            return;
        }
        localCacheDataSource.setScoreWeightRecallCache(cacheKey, resultDocumentList, CACHE_TIME_SECONDS);
        baseDc.minorLanguageRecallList.addAll(resultDocumentList);
        baseDc.resultFromMap.put(this.getName(), DefineTool.RecallFrom.ES.getName());
        baseDc.syncSearchResultSizeMap.put(this.getName(), resultDocumentList.size());
        return;
    }

    /**
     * 构建 ES query
     *
     * @param languageList
     * @return
     */
    private JSONObject constructQuery(List<String> languageList) {
        if (MXJudgeUtils.isEmpty(languageList)) {
            return null;
        }
        JSONObject queryBody = new JSONObject();
        JSONObject bool = new JSONObject();
        JSONArray should = new JSONArray();
        for (String language : languageList) {
            if (MXStringUtils.isNotEmpty(language)) {
                JSONObject item = new JSONObject();
                JSONObject match = new JSONObject();
                match.put("ml_tags", "language_" + language);
                item.put("match", match);
                should.add(item);
            }
        }
        bool.put("should", should);
        queryBody.put("bool", bool);
        return queryBody;
    }

    /**
     * 语言列表剔除大语言
     *
     * @param languageList
     * @return
     */
    private List<String> findMinor(List<String> languageList) {
        if (MXJudgeUtils.isEmpty(languageList)) {
            return null;
        }
        List<String> minorList = new ArrayList<>(MINOR_LANGUAGE_LIST);
        List<String> toDelet = new ArrayList<>();
        for (int i = 0; i < languageList.size(); i++) {
            String language = languageList.get(i);
            if (!minorList.contains(language)) {
                toDelet.add(language);
            }
        }
        if (MXJudgeUtils.isNotEmpty(toDelet)) {
            languageList.removeAll(toDelet);
        }
        return languageList;
    }

    /**
     * 构建cachekey
     *
     * @param languageList
     * @return
     */
    private String constructCacheKey(List<String> languageList) {
        Collections.sort(languageList);
        StringBuilder sb = new StringBuilder();
        for (String language : languageList) {
            sb.append(language);
            sb.append("_");
        }
        sb.deleteCharAt(sb.length() - 1);
        return sb.toString();
    }
}
