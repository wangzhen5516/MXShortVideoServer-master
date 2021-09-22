package mx.j2.recommend.recall.impl;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import mx.j2.recommend.conf.Conf;
import mx.j2.recommend.data_model.data_collection.FeedDataCollection;
import mx.j2.recommend.data_source.ElasticCacheSource;
import mx.j2.recommend.data_source.HttpDataSource;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.BloomUtil;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXJudgeUtils;
import mx.j2.recommend.util.MXStringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author zhongren.li
 * @date 2021-01-08 16:20
 */
public class GetFollowersBoolRefreshNewRecall extends BaseRecall<FeedDataCollection> {

    public static final String PUBLISHER_ID = "publisher_id";
    public static final String REQUEST_URL_FORMAT="/%s/_search?pretty=false";

    private static final String REDIS_KEY = "offline_reco_%s";

    @Override
    public boolean skip(FeedDataCollection baseDc) {
        if(MXStringUtils.isBlank(baseDc.req.getResourceType()) || MXStringUtils.isBlank(baseDc.req.getResourceId())){
            return true;
        }
        return false;
    }

    @Override
    public void recall(FeedDataCollection baseDc){
        /*没拉新时间直接返回有红点*/
        if(MXStringUtils.isBlank(baseDc.req.getLastRefreshTime())){
            baseDc.data.response.setRedDot(true);
            return ;
        }

        ElasticCacheSource elasticCacheSource = MXDataSource.redis();
        String redisKey = String.format(REDIS_KEY, BloomUtil.getUserId(baseDc));
        Map<String, Double> manualControlMap = elasticCacheSource.getManualControltCache(redisKey, 0);
        if (MXJudgeUtils.isEmpty(manualControlMap)) {
            baseDc.data.response.setRedDot(false);
            return;
        }


        /*本地缓存*/
        String localKey = baseDc.req.getResourceId();
        int count = 0;
        List<String> followerListFromCache = MXDataSource.cache().getUserFollowersFromCache(localKey);
        if(MXJudgeUtils.isNotEmpty(followerListFromCache)){
            count = processFollowers(baseDc,followerListFromCache);
            baseDc.data.response.setRedDot(count>0);
            return;
        }
        List<String>followerList = getFollowers(baseDc);
        if(MXJudgeUtils.isEmpty(followerList)){
            baseDc.data.response.setRedDot(false);
            return;
        }
         /*存入本地缓存*/
        MXDataSource.cache().setUserFollowesCache(localKey,followerList);

        count = processFollowers(baseDc,followerList);
        baseDc.data.response.setRedDot(count>0);
    }

    private int processFollowers(FeedDataCollection feedDc , List<String>followers){
        if(MXJudgeUtils.isEmpty(followers)){
            return 0;
        }

        long lastRefreshTime = Long.valueOf(feedDc.req.getLastRefreshTime());
        String queryBody = constructQuery(PUBLISHER_ID,0,followers,lastRefreshTime/1000);
        String elasticSearchRequest = String.format(REQUEST_URL_FORMAT, DefineTool.CategoryEnum.SHORT_VIDEO.getIndexAndType());
        return MXDataSource.videoES().sendSyncOnlyReturnTotal(elasticSearchRequest,queryBody);
    }

    /**
     * 获取关注列表
     * @param baseDc
     * @return
     */
    private List<String>getFollowers(FeedDataCollection baseDc){
        HttpDataSource source = MXDataSource.http();

        Map<String, String> paramsMap = new HashMap<>();
        paramsMap.put("size", String.valueOf(100));
        paramsMap.put("uid", baseDc.req.getResourceId());

        String result = null;
        try {
            result = source.get(Conf.getMxFollowerServerUrl(), paramsMap);
        } catch (Exception e) {
            e.printStackTrace();
        }

        List<String> followerlist = new ArrayList<>();
        if (MXStringUtils.isBlank(result)) {
            return followerlist;
        }
        JSONObject object;
        try{
            object = JSONObject.parseObject(result);
        } catch (Exception e) {
            e.printStackTrace();
            return followerlist;
        }

        if (object.containsKey("list")) {
            JSONArray array = object.getJSONArray("list");
            if (MXJudgeUtils.isEmpty(array)) {
                return followerlist;
            }
            for (int i = 0; i < array.size(); i++) {
                JSONObject o = array.getJSONObject(i);
                if (o.containsKey("id")) {
                    followerlist.add(o.getString("id"));
                }
            }
        }
        return followerlist;
    }

    private  String constructQuery(String key, int size, List<String> idList,long lastTime){
        JSONObject content = new JSONObject();
        /*构建参数*/
        JSONObject query = new JSONObject();
        JSONArray mustArray = new JSONArray();
        JSONObject termsObject = new JSONObject();
        JSONObject matchObject = new JSONObject();
        JSONObject term = new JSONObject();
        JSONObject match = new JSONObject();
        JSONObject range = new JSONObject();
        JSONObject rangeSon=new JSONObject();
        JSONObject boolObject = new JSONObject();
        JSONObject filter = new JSONObject();
        JSONObject sort=new JSONObject();
        JSONObject sortFather=new JSONObject();

        term.put(key,idList);
        termsObject.put("terms",term);
        match.put("status",DefineTool.OnlineStatusesEnum.ONLINE.getIndex());
        matchObject.put("match",match);
        mustArray.add(termsObject);
        mustArray.add(matchObject);
        boolObject.put("must",mustArray);
        rangeSon.put("gt",lastTime);
        range.put("update_time",rangeSon);
        filter.put("range",range);
        boolObject.put("filter",filter);
        sort.put("order","desc");
        sortFather.put("update_time",sort);

        query.put("bool",boolObject);

        content.put("sort",sortFather);
        content.put("size",size);
        content.put("query",query);
        return content.toString();
    }

}
