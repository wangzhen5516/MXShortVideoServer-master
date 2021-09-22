package mx.j2.recommend.prerecall.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_source.ElasticCacheSource;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.recall.impl.BaseRecall;
import mx.j2.recommend.util.MXCollectionUtils;

import java.util.List;
import java.util.Map;

/**
 * @author qiqi
 * 如果实现该功能，需要在recallManager中判断isRecallResultEmpty
 * @date 2021-07-02 11:46
 */
public class PureUserTopPreRecall extends BaseRecall<BaseDataCollection> {

    private static final String REDIS_KEY = "redis_key";

    @Override
    public void registerConfig(Map<String, Class> outConfMap) {
        super.registerConfig(outConfMap);
        outConfMap.put(REDIS_KEY, String.class);
    }

    @Override
    public void recall(BaseDataCollection dc) {

        ElasticCacheSource elasticCacheSource = MXDataSource.redis();
        List<String> ids = elasticCacheSource.getSetFromStg(getRedisKey());
        if (MXCollectionUtils.isEmpty(ids)) {
            return;
        }
        List<BaseDocument> res = MXDataSource.details().get(ids);
        /*recall from redis*/
        if (MXCollectionUtils.isEmpty(res)) {
            return;
        }
        addResult(dc, res);
        dc.syncSearchResultSizeMap.put(this.getName(), res.size());

    }

    private String getRedisKey() {
        return config.getString(REDIS_KEY);
    }

}
