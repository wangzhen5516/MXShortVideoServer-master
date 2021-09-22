package mx.j2.recommend.recall.impl;

public class UserProfileTagTopRandomV5Recall extends UserProfileTagTopRandom2Recall {
    public UserProfileTagTopRandomV5Recall() {
        REDIS_PREFIX = "tophot_ml_tag_v5_";
        TAG_NUM = 4;
    }
}
