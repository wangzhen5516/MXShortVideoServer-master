package mx.j2.recommend.recall.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_source.ElasticCacheSource;
import mx.j2.recommend.data_source.LocalCacheDataSource;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.MXJudgeUtils;

import java.util.List;

public class IPLRecall extends BaseRecall<BaseDataCollection> {
    private final String IPL_REDIS_KEY = "manual_tag_ipl_v1";
    private final String CACHE_KEY = this.getName() + IPL_REDIS_KEY;

    @Override
    public void recall(BaseDataCollection dc) {
        ElasticCacheSource elasticCacheSource = MXDataSource.redis();
        LocalCacheDataSource localCacheDataSource = MXDataSource.cache();

        List<BaseDocument> docList = localCacheDataSource.getIPLCache(CACHE_KEY);
        if (MXJudgeUtils.isEmpty(docList)) {
            List<String> ids = elasticCacheSource.getVideoFeatureZsetInfoFromRedis(IPL_REDIS_KEY);
            if (MXJudgeUtils.isEmpty(ids)) {
                return;
            }

            docList = MXDataSource.details().get(ids, getName());
            if (MXJudgeUtils.isEmpty(docList)) {
                return;
            }

            localCacheDataSource.setIPLCache(CACHE_KEY, docList);
        }
        dc.IPLDocumentList.addAll(docList);
    }
}
