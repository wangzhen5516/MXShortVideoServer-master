package mx.j2.recommend.recall.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.manager.MXDataSource;

/**
 * @author zhendong.wang
 * @version 1.0
 * @date 2021/7/24 下午1:47
 * @description 为创作者召回大 V 数据
 */
@SuppressWarnings("unused")
public class CreatorKOLRecall extends BaseElasticSearchRecall {
    private static final String KEY_REDIS = "publisher_bloom_for_kol_test";

    @Override
    public boolean skip(BaseDataCollection dc) {
        return !isCreator(dc) || super.skip(dc);
    }

    /**
     * 判断是否是创作者
     */
    private boolean isCreator(BaseDataCollection dc) {
        return MXDataSource.inactiveHistoryBloom().existsSingle(KEY_REDIS, dc.getUserInfo().userId);
    }
}
