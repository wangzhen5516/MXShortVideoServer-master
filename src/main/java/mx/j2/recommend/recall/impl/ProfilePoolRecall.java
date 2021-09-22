package mx.j2.recommend.recall.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.pool_conf.ExposurePoolConf;

import java.util.List;
import java.util.Map;

/**
 * 个性化池召回
 */
@SuppressWarnings("unused")
public class ProfilePoolRecall extends ExposurePoolRecall {

    @Override
    ExposurePoolConf getPoolConf(BaseDataCollection dc) {
        return MXDataSource.profilePoolConf().get(dc.recommendFlow.name);
    }

    @Override
    Map<String, ExposurePoolConf> getAllPoolConf() {
        return MXDataSource.profilePoolConf().all();
    }

    @Override
    List<BaseDocument> getPoolData(String key) {
        return MXDataSource.profilePool().get(key);
    }
}
