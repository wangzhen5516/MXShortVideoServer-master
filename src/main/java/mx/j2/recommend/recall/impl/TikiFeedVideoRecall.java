package mx.j2.recommend.recall.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_source.ElasticCacheSource;
import mx.j2.recommend.data_source.LocalCacheDataSource;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXCollectionUtils;

import java.util.List;

/**
 * @author qiqi
 * @date 2021-04-26 17:34
 */
public class TikiFeedVideoRecall extends BaseRecall<BaseDataCollection> {

    private static final String REDIS_KEY = "tiki_feed_reco_id";

    @Override
    public boolean skip(BaseDataCollection dc) {
        return false;
    }

    @Override
    public void recall(BaseDataCollection dc) {

        LocalCacheDataSource localCache = MXDataSource.cache();
        String localKey = String.format("%s_%s", this.getName(), REDIS_KEY);
        List<BaseDocument> localRes = localCache.getTikiCache(localKey);
        if (MXCollectionUtils.isNotEmpty(localRes)) {
            dc.syncSearchResultSizeMap.put(this.getName(), localRes.size());
            dc.resultFromMap.put(this.getName(), DefineTool.RecallFrom.LOCAL.getName());
            return;
        }

        ElasticCacheSource elasticCacheSource = MXDataSource.redis();
        List<String> tikiFeedIds = elasticCacheSource.getSetFromStg(REDIS_KEY);
        if (MXCollectionUtils.isEmpty(tikiFeedIds)) {
            return;
        }
        List<BaseDocument> docList = MXDataSource.details().get(tikiFeedIds, this.getName());
        if (MXCollectionUtils.isEmpty(docList)) {
            return;
        }
        localCache.setTikiCache(localKey, docList);
        dc.syncSearchResultSizeMap.put(this.getName(), docList.size());
        dc.resultFromMap.put(this.getName(), DefineTool.RecallFrom.REDIS.getName());
    }
}

