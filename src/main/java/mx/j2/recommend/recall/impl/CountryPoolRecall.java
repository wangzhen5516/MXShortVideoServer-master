package mx.j2.recommend.recall.impl;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.newrelic.api.agent.Trace;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.recall_data_in_mem.ESCanGetStartRecall;
import mx.j2.recommend.util.BaseMagicValueEnum;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXJudgeUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * @author ：xiang.zhou
 * @date ：Created in 3:41 下午 2021/02/08
 */
public class CountryPoolRecall extends BaseRecall<BaseDataCollection> implements ESCanGetStartRecall{

    private static final Logger log = LogManager.getLogger(CountryPoolRecall.class);

    private final static int RANDOM_FACTOR = new Random().nextInt(10);
    private static final String REQUEST_URL_FORMAT = "/%s/_search?pretty=false";
    private static final int RECALL_WEIGHT = 1200;
    private static final int RECALL_SIZE = 500;
    private static final int RECALL_FROM = 0;
    private static final JSONArray DEFAULT_SORT_JSON;

    private final static String INDEX_PATTERN = "taka_flowpool_language_lv1";


    static {
        DEFAULT_SORT_JSON = new JSONArray();
        JSONObject sortCore = new JSONObject();
        JSONObject script = new JSONObject();
        script.put("script", "Math.random()");
        script.put("type", "number");
        script.put("order", "asc");
        sortCore.put("_script", script);
        DEFAULT_SORT_JSON.add(sortCore);
    }

    @Override
    @Trace(dispatcher = true)
    public void recall(BaseDataCollection baseDc) {
        if (!DefineTool.TabInfoEnum.HOT.getId().equals(baseDc.req.tabId)) {
            return;
        }

        List<BaseDocument> mergedList = MXDataSource.tempLanguagePool().getDocumentList(INDEX_PATTERN);
        for(BaseDocument d : mergedList) {
            if(d!= null) {
                d.setPoolLevel(BaseMagicValueEnum.LOW_LEVEL);
            }
        }

        if (MXJudgeUtils.isNotEmpty(mergedList)) {
            baseDc.countryDocumentList.addAll(mergedList);
            baseDc.syncSearchResultSizeMap.put(this.getName(), mergedList.size());
        }

        baseDc.resultFromMap.put(this.getName(), DefineTool.RecallFrom.ES.getName());
    }

    @Override
    public float getRecallWeightScore() {
        return RECALL_WEIGHT;
    }

    @Override
    public String getRecallName() {
        return this.getName();
    }

    @Override
    public float getRecallDocumentWeight() {
        return 0;
    }

    @Override
    public Map<String, BaseDataCollection.ESRequest> getESRequestMap() {
        Map<String, BaseDataCollection.ESRequest> esRequestMap = new HashMap<>();
        BaseDataCollection.ESRequest esRequest = constructRequest(INDEX_PATTERN);
        esRequestMap.put(INDEX_PATTERN, esRequest);
        return esRequestMap;
    }
    @Override
    public Map<String, SearchSourceBuilder> getSearchSourceBuilderMap() {
        return new HashMap<>();
    }

    @Override
    public void doSomethingAfterLoad() {}

    private BaseDataCollection.ESRequest constructRequest(String index) {

        String elasticSearchRequest = String.format(REQUEST_URL_FORMAT, index);
        String content = constructContent(null, RECALL_FROM, RECALL_SIZE, null, DEFAULT_SORT_JSON).toString();

        if (log.isDebugEnabled()) {
            log.debug(String.format("CountryPoolRecall search url : %s", content));
            log.debug(String.format("CountryPoolRecall search url : %s", elasticSearchRequest));
        }

        return new BaseDataCollection.ESRequest(elasticSearchRequest, content, this.getRecallName(), "", "pool");
    }

    @Override
    public int scheduledPeriodSeconds() {
        return DefineTool.ScheduledPeriodSeconds.TenSeconds.getSeconds();
    }

    @Override
    public int getRandomFactor() {
        return RANDOM_FACTOR;
    }

    @Override
    public boolean skip(BaseDataCollection data) {
        return false;
    }
}
