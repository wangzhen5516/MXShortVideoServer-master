package mx.j2.recommend.recall.impl;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import mx.j2.recommend.conf.Conf;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_model.interfaces.IDocumentProcessor;
import mx.j2.recommend.data_source.ElasticCacheSource;
import mx.j2.recommend.data_source.HttpDataSource;
import mx.j2.recommend.data_source.LocalCacheDataSource;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.BloomUtil;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXJudgeUtils;
import mx.j2.recommend.util.MXStringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FetchLatestFollowersContentNewRecall extends BaseRecall<BaseDataCollection> {
    private final static int CACHE_TIME_SECONDS = 10;

    private static final int RECALL_SIZE = 300;

    private static final String REDIS_KEY = "offline_reco_%s";

    @Override
    public boolean skip(BaseDataCollection baseDc) {
        return MXJudgeUtils.isBlank(baseDc.req.getResourceType()) || MXStringUtils.isBlank(baseDc.req.getResourceId()) || !MXJudgeUtils.isLogin(baseDc);
    }

    @Override
    public void recall(BaseDataCollection baseDc) {
        //召回视频的本地缓存
        String cacheKey = construcCacheKey(baseDc);

        LocalCacheDataSource localCacheDataSource = MXDataSource.cache();
        List<BaseDocument> cacheDocumentList = localCacheDataSource.getScoreWeightRecallCache(cacheKey);

        if (MXJudgeUtils.isNotEmpty(cacheDocumentList)) {
            baseDc.mergedList.addAll(0, cacheDocumentList);
            baseDc.resultFromMap.put(this.getName(), DefineTool.RecallFrom.LOCAL.getName());
            baseDc.syncSearchResultSizeMap.put(this.getName(), cacheDocumentList.size());
            return;
        }

        //获取关注的所有人
        List<String> followerList = getFollowersFromLocalCache(baseDc);
        if (MXJudgeUtils.isEmpty(followerList)) {
            try {
                followerList = getFollowers(baseDc);
            } catch (Exception e) {
                e.printStackTrace();
            }
            if (MXJudgeUtils.isEmpty(followerList)) {
                return;
            }
            setFollowersToLocalCache(baseDc, followerList);
        }


        ElasticCacheSource elasticCacheSource = MXDataSource.redis();
        String redisKey = String.format(REDIS_KEY, BloomUtil.getUserId(baseDc));

        Map<String, Double> manualControlMap = elasticCacheSource.getManualControltCache(redisKey, RECALL_SIZE);
        if (MXJudgeUtils.isEmpty(manualControlMap)) {
            return;
        }

        List<String> idList = new ArrayList<>(manualControlMap.keySet());
        IDocumentProcessor processor = document -> {
            if (manualControlMap.containsKey(document.getId())) {
                document.scoreDocument.manualTopScore = manualControlMap.get(document.getId());
            }
        };
        List<BaseDocument> resultList = MXDataSource.details().get(idList, this.getName(), processor);

        List<BaseDocument> mergedList = new ArrayList<>();
        List<String> finalFollowerList = followerList;
        resultList.forEach(obj -> {
            if (finalFollowerList.contains(obj.getPublisher_id())) {
                mergedList.add(obj);
            }
        });

        mergedList.sort((doc0, doc1) -> Double.compare(doc1.heatScore, doc0.heatScore));
        localCacheDataSource.setScoreWeightRecallCache(cacheKey, mergedList, CACHE_TIME_SECONDS);
        baseDc.mergedList.addAll(0, mergedList);
        baseDc.resultFromMap.put(this.getName(), DefineTool.RecallFrom.ES.getName());
        baseDc.syncSearchResultSizeMap.put(this.getName(), mergedList.size());
    }

    private String construcCacheKey(BaseDataCollection baseDc) {
        return String.format("%s_%s_%s", this.getName(), baseDc.req.getInterfaceName(), BloomUtil.getUserId(baseDc));
    }

    /**
     * 获取用户关注的人
     *
     * @param baseDc
     * @return
     */
    private List<String> getFollowers(BaseDataCollection baseDc) throws Exception {
        HttpDataSource httpDataSource = MXDataSource.http();
        Map<String, String> paramsMap = new HashMap<>();
        paramsMap.put("uid", baseDc.req.getResourceId());
        String url = Conf.getMxFollowerServerUrl();

        String resultString = httpDataSource.get(url, paramsMap);
        List<String> followers = new ArrayList<>();
        if (MXJudgeUtils.isEmpty(resultString)) {
            return followers;
        }
        JSONObject object;
        try {
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

    private List<String> getFollowersFromLocalCache(BaseDataCollection baseDc) {
        String cacheKey = generateFollowerCacheKey(baseDc);
        return MXDataSource.cache().getUserFollowersFromCache(cacheKey);
    }

    private String generateFollowerCacheKey(BaseDataCollection baseDc) {
        return baseDc.req.getResourceId();
    }

    private void setFollowersToLocalCache(BaseDataCollection baseDc, List<String> ids) {
        String cacheKey = generateFollowerCacheKey(baseDc);
        MXDataSource.cache().setUserFollowesCache(cacheKey, ids);
    }
}
