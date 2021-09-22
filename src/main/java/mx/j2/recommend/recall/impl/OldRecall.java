package mx.j2.recommend.recall.impl;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.newrelic.api.agent.Trace;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.util.DefineTool;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * 一级养老池
 * @author qiqi
 * @date 2020-08-24 11:45
 */
@Deprecated
public class OldRecall extends BaseRecall<BaseDataCollection> {

    private static final int RECALL_SIZE = 200;
    private static final String REQUEST_URL_FORMAT = "/%s/_search?pretty=false";
    private static final String INDEX_URL = "taka_flowpool_old";
    private static final int RANDOM = new Random().nextInt(10);
    private static final float RECALL_WEIGHT_SCORE = 1200;
    private static final JSONArray SORT_JSON;

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
    public boolean skip(BaseDataCollection data) {
        return false;
    }

    @Override
    @Trace(dispatcher = true)
    public void recall(BaseDataCollection baseDc){
//        if(!DefineTool.TabInfoEnum.HOT.getId().equals(baseDc.req.getTabId())){
//            return;
//        }
//        List<BaseDocument> mergedList = CanGetAtStartRecallDocumentDataSource.INSTANCE.
//                getDocumentsForESRecall(this, getEsKey());
//        if (CollectionUtils.isEmpty(mergedList)){
//            return;
//        }
//        baseDc.oldLv1List.addAll(mergedList);
//        baseDc.syncSearchResultSizeMap.put(this.getName(), mergedList.size());
//        baseDc.resultFromMap.put(this.getName(), DefineTool.RecallFrom.ES.getName());
    }

    public int scheduledPeriodSeconds() {
        return DefineTool.ScheduledPeriodSeconds.TenSeconds.getSeconds();
    }

    public String getRecallName(){
        return this.getName();
    }
    public void doSomethingAfterLoad(){

    }

    public Map<String , BaseDataCollection.ESRequest> getESRequestMap(){
        Map<String , BaseDataCollection.ESRequest> EsReqMap = new HashMap<>();
        BaseDataCollection.ESRequest esRequest = getEsRequest();
        EsReqMap.put(getEsKey(),esRequest);
        return EsReqMap;
    }

    private BaseDataCollection.ESRequest getEsRequest(){
        String EsRequest = String.format(REQUEST_URL_FORMAT,INDEX_URL);
        String content = constructContent(null,0,RECALL_SIZE,null,SORT_JSON).toJSONString();

        return new BaseDataCollection.ESRequest(EsRequest, content, this.getName(), "", "pool");
    }
    public int getRandomFactor(){
        return RANDOM;
    }

    private String getEsKey(){
        return "DEFAULT";
    }

    public float getRecallDocumentWeight(){
        return 0;
    }

    @Override
    public float getRecallWeightScore(){
        return RECALL_WEIGHT_SCORE;
    }
}
