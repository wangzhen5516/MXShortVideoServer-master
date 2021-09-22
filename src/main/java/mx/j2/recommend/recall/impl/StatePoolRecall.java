package mx.j2.recommend.recall.impl;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.newrelic.api.agent.Trace;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_source.CanGetAtStartRecallDocumentDataSource;
import mx.j2.recommend.recall_data_in_mem.ESCanGetStartRecall;
import mx.j2.recommend.thrift.Location;
import mx.j2.recommend.thrift.Request;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXJudgeUtils;
import mx.j2.recommend.util.MXStringUtils;
import mx.j2.recommend.util.OptionalUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.util.*;

/**
 * @author ：zhongrenli
 * @date ：Created in 3:41 下午 2021/01/25
 */
public class StatePoolRecall extends BaseRecall<BaseDataCollection> implements ESCanGetStartRecall {

    private static final Logger log = LogManager.getLogger(StatePoolRecall.class);

    private static final String REQUEST_URL_FORMAT = "/%s/_search?pretty=false";
    private static final int RECALL_WEIGHT = 1200;
    private static final int RECALL_SIZE = 500;
    private static final int RECALL_FROM = 0;
    private static final JSONArray DEFAULT_SORT_JSON;

    private final static int RANDOM_FACTOR = new Random().nextInt(10);

    private final static String INDEX_PATTERN = "taka_flowpool_lv11_%s_v1";

    private final static Map<String, String> STATE_TO_REGION_MAP = new HashMap<String, String>() {
        {
            put("maharashtra", "western"); put("national_capital_territory_of_delhi", "northern"); put("uttar_pradesh", "central");
            put("gujarat", "western"); put("rajasthan", "northern");put("west_bengal", "eastern"); put("madhya_pradesh", "central");
            put("central", "eastern"); put("karnataka", "northern"); put("telangana", "southern"); put("punjab", "northern");
            put("haryana", "northern"); put("odisha", "eastern"); put("assam", "north_eastern"); put("chhattisgarh", "central");
            put("tamil_nadu", "southern"); put("andhra_pradesh", "southern"); put("jharkhand", "eastern"); put("himachal_pradesh", "northern");
            put("chandigarh", "northern"); put("uttarakhand", "central"); put("jammu_and_kashmir", "northern"); put("kerala", "southern");
            put("sikkim", "north_eastern"); put("manipur", "north_eastern"); put("goa", "western"); put("union_territory_of_puducherry", "southern");
            put("meghalaya", "north_eastern"); put("tripura", "north_eastern"); put("nagaland", "north_eastern"); put("arunachal_pradesh", "north_eastern");
            put("daman_and_diu", "southern"); put("dadra_and_nagar_haveli", "western"); put("mizoram", "north_eastern"); put("andaman_and_nicobar", "southern");
        }
    };

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
    public boolean skip(BaseDataCollection baseDc) {
        if (!DefineTool.TabInfoEnum.HOT.getId().equals(baseDc.req.tabId)) {
            return true;
        }
        return false;
    }

    @Override
    @Trace(dispatcher = true)
    public void recall(BaseDataCollection baseDc) {
        final String[] stateArray = {""};
        OptionalUtil.ofNullable(baseDc.req)
                .getUtil(Request::getLocation)
                .getUtil(Location::getState)
                .ifPresent(state -> stateArray[0] = state);

        if (MXStringUtils.isEmpty(stateArray[0])) {
            return;
        }

        String stateString = getLowerState(stateArray[0]);
        if (!STATE_TO_REGION_MAP.containsKey(stateString)) {
            return;
        }

        String index = STATE_TO_REGION_MAP.get(stateString);
        List<BaseDocument> mergedList = CanGetAtStartRecallDocumentDataSource.INSTANCE.
                getDocumentsForESRecall(this, String.format(INDEX_PATTERN, index));

        if (MXJudgeUtils.isNotEmpty(mergedList)) {
            baseDc.topHotStateList.addAll(mergedList);
            baseDc.syncSearchResultSizeMap.put(this.getName() + index, mergedList.size());
        }

        baseDc.resultFromMap.put(this.getName(), DefineTool.RecallFrom.ES.getName());
    }

    private String getLowerState(String s) {
        return s.toLowerCase().replaceAll(" ", "_");
    }

    @Override
    public float getRecallWeightScore() {
        return RECALL_WEIGHT;
    }

    public static void main(String[] args) {
        System.out.println(new StatePoolRecall().constructContent(null, RECALL_FROM, RECALL_SIZE, null, DEFAULT_SORT_JSON).toString());
    }
    @Override
    public Map<String, SearchSourceBuilder> getSearchSourceBuilderMap() {
        return new HashMap<>();
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
        Set<String> set = new HashSet<>(STATE_TO_REGION_MAP.values());

        for (String region : set) {
            String index = String.format(INDEX_PATTERN, region);
            BaseDataCollection.ESRequest esRequest = constructRequest(index);
            esRequestMap.put(index, esRequest);
        }
        return esRequestMap;
    }

    @Override
    public void doSomethingAfterLoad() {}

    private BaseDataCollection.ESRequest constructRequest(String index) {

        String elasticSearchRequest = String.format(REQUEST_URL_FORMAT, index);
        String content = constructContent(null, RECALL_FROM, RECALL_SIZE, null, DEFAULT_SORT_JSON).toString();

        if (log.isDebugEnabled()) {
            log.debug(String.format("StatePoolRecall search url : %s", content));
            log.debug(String.format("StatePoolRecall search url : %s", elasticSearchRequest));
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
}
