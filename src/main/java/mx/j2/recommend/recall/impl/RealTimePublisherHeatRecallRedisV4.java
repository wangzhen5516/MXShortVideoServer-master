package mx.j2.recommend.recall.impl;

public class RealTimePublisherHeatRecallRedisV4 extends RealTimePublisherHeatRecallRedisV3{

    @Override
    protected void setRedisFormat() {
        REDIS_KEY_FORMAT = "item_reco_cf_04-%s";
    }
}
