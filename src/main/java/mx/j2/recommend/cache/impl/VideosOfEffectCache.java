package mx.j2.recommend.cache.impl;

import com.newrelic.api.agent.Trace;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_source.LocalCacheDataSource;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXStringUtils;

import java.util.List;


/**
 * Cache for MX_VIDEOS_OF_THE_EFFECT_VERSION_1_0
 *
 * @author Qi Mao
 * @Date 1/4/2021
 */

public class VideosOfEffectCache extends BaseCache<BaseDataCollection> {
    private static String PINNED = "pinned";
    private static String NORMAL = "normal";

    @Override
    protected boolean skipRead(BaseDataCollection dc) {
        if (MXStringUtils.isNotEmpty(dc.req.nextToken) || MXStringUtils.isEmpty(dc.req.resourceId) || !"effect".equals(dc.req.resourceType)) {
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

        List<BaseDocument> docListPinned = localCacheDataSource.getVideosOfEffectCache(localCacheKeyPinned);
        List<BaseDocument> docListNormal = localCacheDataSource.getVideosOfEffectCache(localCacheKeyNormal);
        if (null == docListPinned || null == docListNormal) {
            return;
        }

        dc.effectTopVideoList.addAll(docListPinned);
        dc.mergedList.addAll(docListNormal);
        dc.util.cacheStatus = DefineTool.Cache.CacheStatus.IgnoreRecall;
        dc.isFromTagCache = true;
    }

    @Override
    protected boolean skipWrite(BaseDataCollection dc) {
        if (MXStringUtils.isNotEmpty(dc.req.nextToken)
                || MXStringUtils.isEmpty(dc.req.resourceId)
                || !"effect".equals(dc.req.resourceType)
                || null == dc.data.result.resultList) {
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
        localCacheDataSource.setVideosOfEffectCache(localCacheKeyPinned, dc.effectTopVideoList);
        localCacheDataSource.setVideosOfEffectCache(localCacheKeyNormal, dc.mergedList);
    }
}
