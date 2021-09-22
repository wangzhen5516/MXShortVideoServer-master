package mx.j2.recommend.recall.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.data_collection.info.MXEntityDebugInfo;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.hystrix.redis.ZrevRangeWithScoresPvtActCommand;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXJudgeUtils;

import java.util.*;

public class TrendingRealtimeV10WeightedByTime extends RealTimeVideoFromRedisRecall {

    private final String COEFFICIENT_REDIS_KEY = "trending_realtime_coefficient";
    private double coefficient = 0.97;

    @Override
    public String get_key_format() {
        return "%s:similar_video_v10";
    }

    @Override
    public void recall(BaseDataCollection dc) {
        // 获取video_id 和 score(TimeStamp)
        ZrevRangeWithScoresPvtActCommand command = new ZrevRangeWithScoresPvtActCommand(get_redis_key(dc), 0, CUT_DOWN);
        Map<String, Double> idsWithTimestamps = command.execute();
        if (MXJudgeUtils.isEmpty(idsWithTimestamps)) {
            return;
        }

        // 根据TimeStamp对video id进行分组
        Map<Double, List<String>> timeMap = new TreeMap<>();
        for (Map.Entry<String, Double> entry : idsWithTimestamps.entrySet()) {
            List<String> idList = timeMap.getOrDefault(Math.abs(entry.getValue()), new ArrayList<>());
            idList.add(entry.getKey());
            if (idList.size() == 1) {
                timeMap.put(Math.abs(entry.getValue()), idList);
            }
        }

        // 获取视频详情
        List<BaseDocument> docLists = MXDataSource.details().get(idsWithTimestamps.keySet(), this.getName());
        if (MXJudgeUtils.isEmpty(docLists)) {
            return;
        }

        // 按照时间戳倒叙排序，并按照video id为key构建对应rank map
        timeMap = ((TreeMap) timeMap).descendingMap();
        Map<String, Integer> mapWithRank = new HashMap<>();
        int rank = 1;
        for (Map.Entry<Double, List<String>> entry : timeMap.entrySet()) {
            for (String id : entry.getValue()) {
                mapWithRank.put(id, rank);
            }
            rank++;
        }

        // 计算排序所需的值
        docLists.stream().forEach(doc -> {
            doc.setRealTimeSortedWeighted(doc.getHeatScore2() * Math.pow(coefficient, mapWithRank.get(doc.id)));

            // 召回信息
            MXEntityDebugInfo debugInfo = dc.debug.getDebugInfoByEntityId(doc.id);
            debugInfo.recall.name = getName();
        });

        // 按照realTimeSortedWeighted排序
        docLists.sort(Comparator.comparing(BaseDocument::getRealTimeSortedWeighted).reversed());

        dc.similarRealList.addAll(docLists);
        dc.syncSearchResultSizeMap.put(this.getName(), docLists.size());
        dc.resultFromMap.put(this.getName(), DefineTool.RecallFrom.REDIS.getName());
    }
}
