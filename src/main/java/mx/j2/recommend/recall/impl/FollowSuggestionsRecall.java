package mx.j2.recommend.recall.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_source.LocalCacheDataSource;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXJudgeUtils;
import mx.j2.recommend.util.MXStringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FollowSuggestionsRecall extends BaseRecall<BaseDataCollection> {

    private static final int DEFAULT_MAX = 100;
    private static final long CACHE_TIME_SECONDS = 60;
    private static final String REDIS_KEY = "follow_suggestions";

    @Override
    public boolean skip(BaseDataCollection dc) {
        int num = dc.req.getNum();
        if (num < 0 || num > DEFAULT_MAX) {
            return true;
        }
        return false;
    }

    @Override
    public void recall(BaseDataCollection dc) {
        LocalCacheDataSource localCacheDataSource = MXDataSource.cache();
        String cacheKey = constructCacheKey();
        List<BaseDocument> localDocumentList = localCacheDataSource.getScoreWeightRecallCache(cacheKey);

        if (MXJudgeUtils.isNotEmpty(localDocumentList)) {
            dc.mergedList.addAll(localDocumentList);
            dc.resultFromMap.put(this.getName(), DefineTool.RecallFrom.LOCAL.getName());
            dc.syncSearchResultSizeMap.put(this.getName(), localDocumentList.size());
            return;
        }

        List<String> idList = getIdListFromRedis();


        if (MXJudgeUtils.isEmpty(idList)) {
            return;
        }

        List<BaseDocument> mergedList = MXDataSource.details().get(idList, getName());
        if (MXJudgeUtils.isEmpty(mergedList)) {
            return;
        }

        localCacheDataSource.setScoreWeightRecallCache(cacheKey, mergedList, CACHE_TIME_SECONDS);
        dc.mergedList.addAll(mergedList);
        dc.syncSearchResultSizeMap.put(this.getName(), mergedList.size());
        dc.resultFromMap.put(this.getName(), DefineTool.RecallFrom.ES.getName());
    }

    private String constructCacheKey() {
        return String.format("%s_%s", REDIS_KEY, this.getName());
    }

    List<String> getIdListFromRedis() {
        String result = MXDataSource.redis().getString(REDIS_KEY);
        if (MXStringUtils.isEmpty(result)) {
            return null;
        }
        List<String> idList = parseIdList(result);
        if (MXJudgeUtils.isNotEmpty(idList)) {
            return idList;
        }
        return null;
    }

    List<String> parseIdList(String s) {
        if (1 > s.length() - 1) {
            return null;
        }
        s = s.substring(1, s.length() - 1);
        if (MXStringUtils.isEmpty(s)) {
            return null;
        }
        List<String> rtList = new ArrayList<>();
        List<String> idList = Arrays.asList(s.split(","));
        if (MXJudgeUtils.isNotEmpty(idList)) {
            for (String id : idList) {
                id = MXStringUtils.strip(id.replace("\'", ""));
                rtList.add(id);
            }
            return rtList;
        }
        return null;
    }

}