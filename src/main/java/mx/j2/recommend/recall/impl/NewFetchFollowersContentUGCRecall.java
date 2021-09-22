package mx.j2.recommend.recall.impl;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.newrelic.api.agent.Trace;
import mx.j2.recommend.conf.Conf;
import mx.j2.recommend.data_model.data_collection.FeedDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_model.interfaces.IDocumentProcessor;
import mx.j2.recommend.data_source.HttpDataSource;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXJudgeUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NewFetchFollowersContentUGCRecall extends BaseRecall<FeedDataCollection> {
    private JSONArray sortJson;
    private final static int RECALL_SIZE = 100;

    private final static int FOLLOWERS_SIZE = 100;

    private static final String PUBLISHER_ID = "publisher_id";

    /**
     * 构造函数
     */
    public NewFetchFollowersContentUGCRecall() {
        init();
    }

    /**
     *
     * 初始化
     *
     */
    @Override
    public void init() {
        requestUrlFormat = "/%s/_search?pretty=false";

        sortJson = new JSONArray();

        JSONObject sortCore = new JSONObject();
        sortCore.put("order", "desc");
        sortCore.put("missing", "_last");
        JSONObject sortObj = new JSONObject();
        sortObj.put("heat_score", sortCore);
        sortJson.add(sortObj);
    }

    @Override
    public boolean skip(FeedDataCollection baseDc) {
        if (MXJudgeUtils.isEmpty(baseDc.req.getResourceId())) {
            return true;
        }

        if (MXJudgeUtils.isEmpty(baseDc.req.getResourceType())) {
            return true;
        }
        return false;
    }

    @Override
    @Trace(dispatcher = true)
    public void recall(FeedDataCollection baseDc){
        List<String> followerListFromCache = getFollowersFromLocalCache(baseDc);

        if(followerListFromCache!=null){
            processFollowers(baseDc, followerListFromCache);
            return;
        }

        // 关注接口
        List<String> followerList = getFollowers(baseDc);
        if (null == followerList ) {
            return;
        }

        setFollowersToLocalCache(baseDc, followerList);

        if(followerList.isEmpty()){
            return;
        }

        processFollowers(baseDc, followerList);
    }

    @Trace(dispatcher = true)
    private void processFollowers(FeedDataCollection dc, List<String> followerList){

        if(MXJudgeUtils.isEmpty(followerList)){
            return;
        }

        //List<String> esFollowers = new ArrayList<>(followerList);

        IDocumentProcessor processor = document -> document.scoreDocument.baseScore = 8;
        String queryBody = constructQuery(PUBLISHER_ID, RECALL_SIZE, followerList, null);
        List<BaseDocument> mergedList = MXDataSource.videoES()
                .syncLoadBySearchQyery(queryBody, DefineTool.CategoryEnum.SHORT_VIDEO.getIndexAndType(), this.getName(), processor);

        if(MXJudgeUtils.isNotEmpty(mergedList)){
            dc.isNeedToSetToFetchFollowersBloom = true;
//            Collections.shuffle(mergedList);
        }
        dc.mergedList.addAll(mergedList);
        dc.syncSearchResultSizeMap.put(this.getName(), mergedList.size());
        dc.resultFromMap.put(this.getName(), DefineTool.RecallFrom.ES.getName());

    }

    @Trace(dispatcher = true)
    private List<String> getFollowers(FeedDataCollection baseDc) {
        HttpDataSource httpDataSource = MXDataSource.http();

        Map<String, String> paramsMap = new HashMap<>();
        paramsMap.put("size", String.valueOf(FOLLOWERS_SIZE));
        paramsMap.put("uid", baseDc.req.getResourceId());

        String url = Conf.getMxFollowerServerUrl();
        String resultString = null;
        try {
            resultString = httpDataSource.get(url, paramsMap);
        } catch (Exception e) {
            e.printStackTrace();
        }

        List<String> followers = new ArrayList<>();
        if (MXJudgeUtils.isEmpty(resultString)) {
            return followers;
        }
        JSONObject object;
        try{
            object = JSONObject.parseObject(resultString);
        } catch (Exception e) {
            e.printStackTrace();
            return followers;
        }

        if (object.containsKey("list")) {
            JSONArray array = object.getJSONArray("list");
            if (MXJudgeUtils.isEmpty(array)) {
                return followers;
            }
            for (int i = 0; i < array.size(); i++) {
                JSONObject o = array.getJSONObject(i);
                if (o.containsKey("id")) {
                    followers.add(o.getString("id"));
                }
            }
        }
        return followers;
    }

    private List<String> getFollowersFromLocalCache(FeedDataCollection baseDc){
        String cacheKey = generateFollowerCacheKey(baseDc);
        return MXDataSource.cache().getUserFollowersFromCache(cacheKey);
    }

    private void setFollowersToLocalCache(FeedDataCollection baseDc, List<String> ids){
        String cacheKey = generateFollowerCacheKey(baseDc);
        MXDataSource.cache().setUserFollowesCache(cacheKey, ids);
    }

    private String generateFollowerCacheKey(FeedDataCollection baseDc){
        String userID = baseDc.req.getResourceId();
        return userID;
    }

    public String constructQuery(String key, int size, List<String> idList, List<String> sourceList){
        JSONObject content = new JSONObject();
        JSONArray mustArray = new JSONArray();
        JSONObject mustFather = new JSONObject();
        JSONObject boolFather = new JSONObject();
        JSONObject termsFather = new JSONObject();
        JSONObject terms = new JSONObject();
        JSONObject matchFather = new JSONObject();
        JSONObject matchField = new JSONObject();
        JSONObject ugcMatchFather = new JSONObject();
        JSONObject ugcMatch = new JSONObject();
        JSONObject matchNotField = new JSONObject();
        JSONObject matchNotFather = new JSONObject();

        String termsKey = "_id";
        if (MXJudgeUtils.isNotEmpty(key)) {
            termsKey = key;
        }

        terms.put(termsKey, idList);
        termsFather.put("terms", terms);

        matchField.put("status", DefineTool.OnlineStatusesEnum.ONLINE.getIndex());
        matchFather.put("match", matchField);

        ugcMatch.put("is_ugc_content", true);
        ugcMatchFather.put("match", ugcMatch);

        mustArray.add(termsFather);
        mustArray.add(matchFather);
        mustArray.add(ugcMatchFather);

        matchNotField.put("view_privacy", 2);
        matchNotFather.put("match", matchNotField);

        mustFather.put("must", mustArray);
        mustFather.put("must_not", matchNotFather);
        boolFather.put("bool", mustFather);
        content.put("query", boolFather);
        content.put("size", size);
        content.put("sort", sortJson);

        if(MXJudgeUtils.isNotEmpty(sourceList)){
            JSONArray sourceArray = new JSONArray();
            sourceArray.addAll(sourceList);
            content.put("_source", sourceArray);
        }
        return content.toString();
    }

    public static void main(String[]args){
        NewFetchFollowersContentUGCRecall recall = new NewFetchFollowersContentUGCRecall();
    }
}
