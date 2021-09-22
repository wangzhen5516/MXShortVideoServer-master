package mx.j2.recommend.cache.impl;

import com.newrelic.api.agent.Trace;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_source.LocalCacheDataSource;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXStringUtils;

import javax.annotation.concurrent.ThreadSafe;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Cache for MX_VIDEOS_OF_THE_SAME_AUDIO_VERSION_1_0
 *
 * @author DuoZhao
 * @Date 2020/11/17 16:14
 */
@ThreadSafe
public class VideosOfSameAudioCache extends BaseCache<BaseDataCollection> {
    private final String DOCUMENT_KEY = "document";
    private final String TOTAL_KEY = "total";

    @Override
    protected boolean skipRead(BaseDataCollection dc) {
        if (MXStringUtils.isNotEmpty(dc.req.nextToken) || MXStringUtils.isEmpty(dc.req.resourceId) || !"audio".equals(dc.req.resourceType)) {
            return true;
        }
        return false;
    }

    @Override
    @Trace(dispatcher = true)
    public void read(BaseDataCollection dc) {
        String localCacheKey = String.format("%s_%s", this.getName(), dc.req.resourceId);
        LocalCacheDataSource localCacheDataSource = MXDataSource.cache();

        Map<String, Object> map = localCacheDataSource.getVideosOfAudioCache(localCacheKey);
        if (null == map) {
            return;
        }
        dc.mergedList.addAll((List<BaseDocument>) map.get(DOCUMENT_KEY));
        dc.totalNumber = (int) map.get(TOTAL_KEY);
        dc.util.cacheStatus = DefineTool.Cache.CacheStatus.IgnoreRecall;
        dc.isFromSameAudioCache = true;
    }

    @Override
    protected boolean skipWrite(BaseDataCollection dc) {
        if (MXStringUtils.isNotEmpty(dc.req.nextToken)
                || MXStringUtils.isEmpty(dc.req.resourceId)
                || !"audio".equals(dc.req.resourceType)
                || null == dc.data.result.resultList) {
            return true;
        }
        return false;
    }

    @Override
    @Trace(dispatcher = true)
    public void write(BaseDataCollection dc) {
        String localCacheKey = String.format("%s_%s", this.getName(), dc.req.resourceId);
        LocalCacheDataSource localCacheDataSource = MXDataSource.cache();
        Map<String, Object> map = new HashMap<>();
        map.put(DOCUMENT_KEY, dc.mergedList);
        map.put(TOTAL_KEY, dc.totalNumber);
        localCacheDataSource.setVideosOfAudioCache(localCacheKey, map);
    }
}
