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
 * Cache for MX_VIDEOS_OF_THE_TAG_VERSION_1_0
 *
 * @author DuoZhao
 * @Date 2020/11/17 16:14
 */
@ThreadSafe
public class VideosOfTagCache extends BaseCache<BaseDataCollection> {
    private static String PINNED = "pinned";
    private static String NORMAL = "normal";

    @Override
    protected boolean skipRead(BaseDataCollection dc) {
        if (MXStringUtils.isNotEmpty(dc.req.nextToken) || MXStringUtils.isEmpty(dc.req.resourceId) || !"tag".equals(dc.req.resourceType)
                || !MXDataSource.hottestHashTag().isHotest(dc.req.resourceId)) {
            return true;
        }
        return false;
    }

    @Override
    @Trace(dispatcher = true)
    public void read(BaseDataCollection dc) {
        String localCacheKeyPinned = String.format("%s_%s_%s_%s", this.getName(), dc.req.resourceId, PINNED, dc.req.num);
        String localCacheKeyNormal = String.format("%s_%s_%s_%s", this.getName(), dc.req.resourceId, NORMAL, dc.req.num);
        LocalCacheDataSource localCacheDataSource = MXDataSource.cache();

        List<BaseDocument> docListPinned = localCacheDataSource.getVideosOfTagCache(localCacheKeyPinned);
        List<BaseDocument> docListNormal = localCacheDataSource.getVideosOfTagCache(localCacheKeyNormal);
        if (null == docListPinned || null == docListNormal) {
            return;
        }

        dc.hashTagTopVideoList.addAll(docListPinned);
        dc.mergedList.addAll(docListNormal);
        dc.util.cacheStatus = DefineTool.Cache.CacheStatus.IgnoreRecall;
        dc.isFromTagCache = true;
    }

    @Override
    protected boolean skipWrite(BaseDataCollection dc) {
        if (MXStringUtils.isNotEmpty(dc.req.nextToken)
                || MXStringUtils.isEmpty(dc.req.resourceId)
                || !"tag".equals(dc.req.resourceType)
                || null == dc.data.result.resultList
                || !MXDataSource.hottestHashTag().isHotest(dc.req.resourceId)) {
            return true;
        }
        return false;
    }

    @Override
    @Trace(dispatcher = true)
    public void write(BaseDataCollection dc) {
        String localCacheKeyPinned = String.format("%s_%s_%s_%s", this.getName(), dc.req.resourceId, PINNED, dc.req.num);
        String localCacheKeyNormal = String.format("%s_%s_%s_%s", this.getName(), dc.req.resourceId, NORMAL, dc.req.num);
        LocalCacheDataSource localCacheDataSource = MXDataSource.cache();
        localCacheDataSource.setVideosOfTagCache(localCacheKeyPinned, dc.hashTagTopVideoList);
        localCacheDataSource.setVideosOfTagCache(localCacheKeyNormal, dc.mergedList);
    }
}

