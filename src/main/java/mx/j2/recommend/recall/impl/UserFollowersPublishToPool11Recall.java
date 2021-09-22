package mx.j2.recommend.recall.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.pool_conf.PoolConf;

import java.util.Collections;
import java.util.List;

/**
 * 用户个性化
 * 大 V 版本
 */
public class UserFollowersPublishToPool11Recall extends UserFollowersPublishRecall {

    public UserFollowersPublishToPool11Recall() {
        REDIS_KEY = "offline_reco_high_pool_v1_%s";
    }

    @Override
    protected List<BaseDocument> getList(BaseDataCollection baseDc) {
        PoolConf conf = MXDataSource.pools().getPoolConfByLevel(11, baseDc);
        if(conf == null) {
            return null;
        }
        return baseDc.poolToDocumentListMap.get(conf.poolIndex);
    }

    @Override
    protected void doSomethingAfterRecall(BaseDataCollection baseDc) {
        if(getList(baseDc) != null) {
            Collections.shuffle(getList(baseDc));
        }
    }
}
