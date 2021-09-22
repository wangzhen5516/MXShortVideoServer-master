package mx.j2.recommend.recall.impl;

/**
 * @author xiang.zhou
 * @date 2021-03-08 17:17
 */
public class TrendingRealtimeV10 extends RealTimeVideoFromRedisRecall {

    public final static TrendingRealtimeV10 INSTANCE = new TrendingRealtimeV10();

    @Override
    public String get_key_format() {
        return "%s:similar_video_v10";
    }
}
