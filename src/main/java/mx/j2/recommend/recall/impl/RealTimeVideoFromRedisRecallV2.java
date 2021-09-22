package mx.j2.recommend.recall.impl;

import mx.j2.recommend.data_model.data_collection.FeedDataCollection;

/**
 * @author xiang.zhou
 * @date 2021-03-08 17:17
 */
public class RealTimeVideoFromRedisRecallV2 extends RealTimeVideoFromRedisRecall {

    @Override
    public String get_key_format() {
        return "%s:similar_video_v2";
    }

    public static void main(String[] args) {
        new RealTimeVideoFromRedisRecallV2().recall(new FeedDataCollection());
    }
}
