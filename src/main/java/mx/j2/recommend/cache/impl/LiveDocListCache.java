package mx.j2.recommend.cache.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.LiveDocument;
import mx.j2.recommend.data_source.LocalCacheDataSource;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXCollectionUtils;

import java.util.List;


/**
 * @author qiqi
 * @date 2021-04-25 17:15
 */
public class LiveDocListCache extends BaseCache<BaseDataCollection> {


    @Override
    protected boolean skipWrite(BaseDataCollection dc) {
        return MXCollectionUtils.isEmpty(dc.liveDocumentList);
    }

    @Override
    protected boolean skipRead(BaseDataCollection dc) {
        return false;
    }

    @Override
    public void read(BaseDataCollection data) {
        String localKey = String.format("%s_%s", this.getName(), data.req.interfaceName);
        LocalCacheDataSource dataSource = MXDataSource.cache();
        List<LiveDocument> localList = dataSource.getLiveCache(localKey);
        if (MXCollectionUtils.isEmpty(localList)) {
            return;
        }
        data.liveDocumentList.addAll(localList);
        data.util.cacheStatus = DefineTool.Cache.CacheStatus.IgnoreRecall;
    }

    @Override
    public void write(BaseDataCollection data) {
        String localKey = String.format("%s_%s", this.getName(), data.req.interfaceName);
        LocalCacheDataSource dataSource = MXDataSource.cache();
        dataSource.setLiveCache(localKey, data.liveDocumentList);
    }
}
