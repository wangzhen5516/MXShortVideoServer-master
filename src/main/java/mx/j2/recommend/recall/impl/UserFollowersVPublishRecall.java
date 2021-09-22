package mx.j2.recommend.recall.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;

import java.util.List;

/**
 * 用户个性化
 * 大 V 版本
 */
public class UserFollowersVPublishRecall extends UserFollowersPublishRecall {

    public UserFollowersVPublishRecall() {
        REDIS_KEY = "offline_reco_V_%s";
    }

    @Override
    protected List<BaseDocument> getList(BaseDataCollection baseDc) {
        return baseDc.userFollowVPublishList;
    }
}
