package mx.j2.recommend.recall.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.pool_conf.PoolConf;

import java.util.Collections;
import java.util.List;

/**
 * "召回 3 个 tag" 版本
 */
@SuppressWarnings("unused")
public class UserProfileTagTopToPool11Recall extends UserProfileTagTopRecall {

    public UserProfileTagTopToPool11Recall() {
        TAG_NUM = 3;
        String REDIS_PREFIX = "tophot_ml_tag_v2_";
    }

    @Override
    protected void doSomethingAfterRecall(BaseDataCollection baseDc) {
        PoolConf conf = MXDataSource.pools().getPoolConfByLevel(11, baseDc);
        if(conf == null) {
            return ;
        }
        List<BaseDocument> toAdd = baseDc.poolToDocumentListMap.get(conf.poolIndex);
        if(toAdd == null) {
            return ;
        }
        if(baseDc.userProfileTagMap != null) {
            baseDc.userProfileTagMap.values().forEach(toAdd::addAll);
        }
        Collections.shuffle(toAdd);

        baseDc.userProfileTagMap.clear();
    }
}
