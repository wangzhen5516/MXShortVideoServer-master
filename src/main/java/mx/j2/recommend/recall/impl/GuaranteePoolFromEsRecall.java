package mx.j2.recommend.recall.impl;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.newrelic.api.agent.Trace;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_source.CanGetAtStartRecallDocumentDataSource;
import mx.j2.recommend.recall_data_in_mem.ESCanGetStartRecall;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXJudgeUtils;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * @author ：zhongrenli
 * @date ：Created in 4:23 下午 2020/11/13
 */
public class GuaranteePoolFromEsRecall extends BaseRecall<BaseDataCollection> implements ESCanGetStartRecall {

    private static final String REQUEST_URL_FORMAT = "/%s/_search?pretty=false";
    private static final int RECALL_WEIGHT = 1200;
    private static final int RECALL_SIZE = 500;
    private static final int RECALL_FROM = 0;
    private static final JSONArray SORT_JSON;

    private static final String INDEX_AND_URL = "taka_flowpool_special_base";

    private final static int RANDOM_FACTOR = new Random().nextInt(10);

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

    public GuaranteePoolFromEsRecall() {}

    @Override
    public boolean skip(BaseDataCollection data) {
        return false;
    }

    @Override
    @Trace(dispatcher = true)
    public void recall(BaseDataCollection dc) {
        List<BaseDocument> documentList = CanGetAtStartRecallDocumentDataSource.INSTANCE.getDocumentsForESRecall(this, getESRequestKey());
        if(MXJudgeUtils.isNotEmpty(documentList)){
            dc.guaranteeFirstLevelDocList.addAll(documentList);
            dc.resultFromMap.put(this.getName(), DefineTool.RecallFrom.ES.getName());
            dc.syncSearchResultSizeMap.put(this.getName(), documentList.size());
        }
    }

    private BaseDataCollection.ESRequest constructESRequest(){
        String elasticSearchRequest = String.format(REQUEST_URL_FORMAT, INDEX_AND_URL);

        JSONObject queryBody = constructESQueryBody();
        JSONObject queryJSON = constructESQuery(queryBody);

        BaseDataCollection.ESRequest esRequest = new BaseDataCollection.ESRequest(
                elasticSearchRequest,
                queryJSON.toString(),
                this.getName(),
                "", DefineTool.EsType.ES_POOL.getTypeName());
        return esRequest;
    }

    JSONObject constructESQueryBody() {
        JSONObject query = new JSONObject();
        query.put("match_all", new JSONObject());

        return query;
    }

    JSONObject constructESQuery(JSONObject query){
        JSONObject content = new JSONObject();
        content.put("query", query);
        content.put("from", RECALL_FROM);
        content.put("size", RECALL_SIZE);
        if (MXJudgeUtils.isNotEmpty(SORT_JSON)) {
            content.put("sort", SORT_JSON);
        }

        return content;
    }

    @Override
    public String getRecallName() {
        return this.getName();
    }

    @Override
    public float getRecallDocumentWeight() {
        return RECALL_WEIGHT;
    }

    @Override
    public Map<String, BaseDataCollection.ESRequest> getESRequestMap() {
        Map<String, BaseDataCollection.ESRequest> esRequestMap = new HashMap<>();
        BaseDataCollection.ESRequest esRequest = constructESRequest();
        esRequestMap.put(getESRequestKey(), esRequest);
        return esRequestMap;
    }

    public String getESRequestKey(){
        return "DEFAULT";
    }

    @Override
    public Map<String, SearchSourceBuilder> getSearchSourceBuilderMap() {
        return new HashMap<>();
    }

    public static void main(String[] args) {
        GuaranteePoolFromEsRecall recall = new GuaranteePoolFromEsRecall();
        recall.getESRequestMap();
    }
    @Override
    public void doSomethingAfterLoad() {}

    @Override
    public int scheduledPeriodSeconds() {
        return DefineTool.ScheduledPeriodSeconds.TenMinutes.getSeconds();
    }

    @Override
    public int getRandomFactor() {
        return RANDOM_FACTOR;
    }

}
