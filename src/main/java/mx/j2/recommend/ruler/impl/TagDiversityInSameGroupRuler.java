package mx.j2.recommend.ruler.impl;

import com.newrelic.api.agent.Trace;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.thrift.Result;
import mx.j2.recommend.util.MXJudgeUtils;
import org.apache.commons.lang.math.JVMRandom;

import java.util.*;

public class TagDiversityInSameGroupRuler extends BaseRuler<BaseDataCollection> {
    private static final Map<String, Integer> tagNumMap;
    private static final String OTHER_TYPE_TAG = "OTHER";
    private static final int INIT_SIZE = 100;
    private static final int EVERY_GROUP_SIZE = 10;
    private static final int EVERY_OTHER_TYPE_GROUP_SIZE = 2;
    static {
        tagNumMap = new HashMap<>();
        tagNumMap.put("beautiful-girl", 3);
        tagNumMap.put("lip-sync", 2);
        tagNumMap.put("dance", 2);
        tagNumMap.put("fashion-style", 1);
        tagNumMap.put("singing", 1);
        tagNumMap.put("trick", 1);
    }

    @Override
    public boolean skip(BaseDataCollection data) {
        return false;
    }

    @Trace(dispatcher = true)
    @Override
    public void rule(BaseDataCollection dc) {
        Set<Float> scoreSet = dc.scoreToResultListMap.keySet();
        for (Float score : scoreSet){
            Map<String, List<Result>> resultSplitByTag = new HashMap<>();
            for (String key : tagNumMap.keySet()){
                resultSplitByTag.put(key, new ArrayList<>(INIT_SIZE));
            }
            resultSplitByTag.put(OTHER_TYPE_TAG, new ArrayList<>(INIT_SIZE));

            Map<String, Integer> currentPositionInTagResultList = new HashMap<>();
            for (String key : tagNumMap.keySet()){
                currentPositionInTagResultList.put(key, 0);
            }
            currentPositionInTagResultList.put(OTHER_TYPE_TAG, 0);

            List<Result> resultListInSameScore = dc.scoreToResultListMap.get(score);
            for (Result result : resultListInSameScore){
                String id = result.shortVideo.id;
                List<String> tags = dc.videoIdToTagListMap.get(id);
                if(MXJudgeUtils.isNotEmpty(tags)){
                    String tag = tags.get(0);
                    if(resultSplitByTag.containsKey(tag)){
                        resultSplitByTag.get(tag).add(result);
                    }else {
                        resultSplitByTag.get(OTHER_TYPE_TAG).add(result);
                    }
                }else {
                    resultSplitByTag.get(OTHER_TYPE_TAG).add(result);
                }
            }
            for(String key : resultSplitByTag.keySet()){
                List<String> ids = new ArrayList<>();
                List<Result> results = resultSplitByTag.get(key);
                for (Result result : results){
                    ids.add(result.shortVideo.id);
                }
//                log.error("7777777 userid "+dc.req.userInfo.uuid+" key "+key+" "+ids);
//                log.error("7777777 userid "+dc.req.userInfo.uuid);
            }

            int groupCount = 0;
            List<Result> resultsOfTheSameScoreAfterRuler = new ArrayList<>(resultListInSameScore.size());
            while (resultsOfTheSameScoreAfterRuler.size()<resultListInSameScore.size()){
                List<Result> groupResultList = new ArrayList<>();

                //每组处理
                List<String> everyGroupId = new ArrayList<>();
                int everyGroupSize = 0;
                //对每个类型的数据处理
                for(String tagType : tagNumMap.keySet()){
                    List<Result> tagResultList = resultSplitByTag.get(tagType);
                    int maxNumInEveryGroup = tagNumMap.get(tagType);
                    //某一个type已经存入最终结果的数量
                    int currentPosition = currentPositionInTagResultList.get(tagType);
                    int addedNum = 0;
                    for (int j = 0; (currentPosition<tagResultList.size()) && (j<maxNumInEveryGroup) ; j++,currentPosition++) {
                        groupResultList.add(tagResultList.get(currentPosition));
                        everyGroupSize++;
                        everyGroupId.add(tagResultList.get(currentPosition).shortVideo.id);
                        addedNum++;
                    }
//                    log.error("7777777 userid "+dc.req.userInfo.uuid+"tagType "+tagType+" "+addedNum);
                    currentPositionInTagResultList.put(tagType, currentPosition);
                }
                {
                    //如果每一组各个指定类型数据不足采用其他类型填充
                    int currentOtherTypeTagPosition = currentPositionInTagResultList.get(OTHER_TYPE_TAG);
                    List<Result> otherTypeTagResultList = resultSplitByTag.get(OTHER_TYPE_TAG);
                    while ((everyGroupSize<EVERY_GROUP_SIZE) && (currentOtherTypeTagPosition<otherTypeTagResultList.size())){
                        Result otherResult = otherTypeTagResultList.get(currentOtherTypeTagPosition);
                        everyGroupId.add(otherResult.shortVideo.id);
                        groupResultList.add(otherResult);
                        everyGroupSize++;
                        currentOtherTypeTagPosition++;
                    }

                    //同时添加其他类型的数据进行shuffle
                    int otherGroupSize = 0;
                    while ((otherGroupSize<EVERY_OTHER_TYPE_GROUP_SIZE) && (currentOtherTypeTagPosition<otherTypeTagResultList.size())){
                        Result otherResult = otherTypeTagResultList.get(currentOtherTypeTagPosition);
                        everyGroupId.add(otherResult.shortVideo.id);
                        groupResultList.add(otherResult);
                        otherGroupSize++;
                        currentOtherTypeTagPosition++;
                    }
                    currentPositionInTagResultList.put(OTHER_TYPE_TAG, currentOtherTypeTagPosition);
                    Collections.shuffle(groupResultList, new JVMRandom());
                    resultsOfTheSameScoreAfterRuler.addAll(groupResultList);
                }

                groupCount++;
//                log.error("7777777 score "+score+" userid "+dc.req.userInfo.uuid+"Every group "+groupCount+" "+everyGroupId);
//                log.error("7777777 score "+score+" userid "+dc.req.userInfo.uuid+"current position "+currentPositionInTagResultList);

                boolean isDesignedTypeNotEmpty = false;
                //如果所有要求的类型都没有数据了用其他类型填充
                for(String tagType : tagNumMap.keySet()){
                    int currentPosition = currentPositionInTagResultList.get(tagType);
                    if(currentPosition<resultSplitByTag.get(tagType).size()){
                        isDesignedTypeNotEmpty = true;
                    }
                }

                if(!isDesignedTypeNotEmpty){
                    int currentOtherTypeTagPosition = currentPositionInTagResultList.get(OTHER_TYPE_TAG);
                    List<Result> otherTypeTagResultList = resultSplitByTag.get(OTHER_TYPE_TAG);
                    while (currentOtherTypeTagPosition<otherTypeTagResultList.size()){
                        resultsOfTheSameScoreAfterRuler.add(otherTypeTagResultList.get(currentOtherTypeTagPosition));
                        currentOtherTypeTagPosition++;
                    }
                    currentPositionInTagResultList.put(OTHER_TYPE_TAG, currentOtherTypeTagPosition);
                }
            }

//            List<String> ids = new ArrayList<>();
//            for (Result result : resultsOfTheSameScoreAfterRuler){
//                ids.add(result.shortVideo.id);
//            }
//            log.error("7777777 userid "+dc.req.userInfo.uuid+" score"+ score+" ids "+ids);

//            log.error("7777777 userid "+dc.req.userInfo.uuid);
//            log.error("7777777 userid "+dc.req.userInfo.uuid);

            dc.scoreToResultListMap.put(score, resultsOfTheSameScoreAfterRuler);
        }

//        Map<Float, List<String>> scoreMap = new HashMap<>();
//        for (Float score : dc.scoreToResultListMap.keySet()){
//            List<String> ids = new ArrayList<>();
//            for (Result result:dc.scoreToResultListMap.get(score)){
//                ids.add(result.shortVideo.id);
//            }
//            scoreMap.put(score, ids);
//        }
//        log.error("7777777 userid "+dc.req.userInfo.uuid+"dc score Map "+scoreMap);
    }
}
