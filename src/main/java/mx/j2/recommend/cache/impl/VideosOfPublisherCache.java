package mx.j2.recommend.cache.impl;

import com.newrelic.api.agent.Trace;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_source.LocalCacheDataSource;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXStringUtils;

import javax.annotation.concurrent.ThreadSafe;
import java.util.List;


/**
 * Cache for MX_VIDEOS_OF_THE_PUBLISHER_VERSION_1_0
 *
 * @author DuoZhao
 * @Date 2020/11/17 16:14
 */
@ThreadSafe
public class VideosOfPublisherCache extends BaseCache<BaseDataCollection> {

    @Override
    protected boolean skipRead(BaseDataCollection dc) {
        if (MXStringUtils.isNotEmpty(dc.req.nextToken) || MXStringUtils.isEmpty(dc.req.resourceId) ||
                dc.req.resourceId.equals(dc.req.userInfo.userId) ||
                !"publisher".equals(dc.req.resourceType)) {
            return true;
        }
        return false;
    }

    @Override
    @Trace(dispatcher = true)
    public void read(BaseDataCollection dc) {
        String localCacheKey = String.format("%s_%s", this.getName(), dc.req.resourceId);
        LocalCacheDataSource localCacheDataSource = MXDataSource.cache();

        List<BaseDocument> docList = localCacheDataSource.getVideosOfPublisherCache(localCacheKey);
        if (null == docList) {
            return;
        }
        dc.mergedList.addAll(docList);
        dc.util.cacheStatus = DefineTool.Cache.CacheStatus.IgnoreRecall;
        dc.isFromPublisherCache = true;
    }

    @Override
    protected boolean skipWrite(BaseDataCollection dc) {
        if (MXStringUtils.isNotEmpty(dc.req.nextToken) || MXStringUtils.isEmpty(dc.req.resourceId) ||
                dc.req.resourceId.equals(dc.req.userInfo.userId) ||
                !"publisher".equals(dc.req.resourceType) || null == dc.data.result.resultList) {
            return true;
        }
        return false;
    }

    @Override
    @Trace(dispatcher = true)
    public void write(BaseDataCollection dc) {
        String localCacheKey = String.format("%s_%s", this.getName(), dc.req.resourceId);
        LocalCacheDataSource localCacheDataSource = MXDataSource.cache();
        localCacheDataSource.setVideosOfPublisherCache(localCacheKey, dc.mergedList);
    }
}
