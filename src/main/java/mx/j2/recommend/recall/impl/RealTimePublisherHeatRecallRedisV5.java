package mx.j2.recommend.recall.impl;

public class RealTimePublisherHeatRecallRedisV5 extends RealTimePublisherHeatRecallRedisV3 {

    @Override
    protected void setRedisFormat() {
        REDIS_KEY_FORMAT = "item_reco_cf_05-%s";
    }
}
