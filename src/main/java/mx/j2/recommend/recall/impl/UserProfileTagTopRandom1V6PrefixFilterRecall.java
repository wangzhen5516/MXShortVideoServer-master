package mx.j2.recommend.recall.impl;

/**
 * change redis key to v6
 */
public class UserProfileTagTopRandom1V6PrefixFilterRecall extends UserProfileTagTopRandom1PrefixFilterRecall {

    public UserProfileTagTopRandom1V6PrefixFilterRecall() {
        REDIS_PREFIX = "tophot_ml_tag_v6_";
    }
}
