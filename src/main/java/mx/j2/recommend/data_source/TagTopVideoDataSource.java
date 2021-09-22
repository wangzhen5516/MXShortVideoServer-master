package mx.j2.recommend.data_source;

import com.alicp.jetcache.Cache;
import com.alicp.jetcache.embedded.CaffeineCacheBuilder;
import com.newrelic.api.agent.Trace;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.hystrix.redis.ZrevRangeStrategyCommand;
import mx.j2.recommend.manager.MXDataSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author ：xuejian.zhang
 * @date ：Created in 20:10 下午 2020/11/10
 */
public class TagTopVideoDataSource extends BaseDataSource {

    private static Logger log = LogManager.getLogger(TagTopVideoDataSource.class);

    private static final int TAG_VOLUME = 10000;
    private static final int TAG_ALIVE_TIME = 60;
    private static final int VIDEO_CUTDOWN = 200;
    private static final String RECALL_NAME = "TagRecall";

    private final Cache<String, List<BaseDocument>> tagCacheInOrder = CaffeineCacheBuilder.createCaffeineCacheBuilder()
            .limit(TAG_VOLUME)
            .expireAfterWrite(TAG_ALIVE_TIME, TimeUnit.MINUTES)
            .cacheNullValue(true)
            .buildCache();

    /**
     * @param tag
     * @return
     */
    private String getRedisKey(String prefix, String tag) {
        return prefix + tag;
    }

    private List<BaseDocument> getVideosFromRedis(String prefix, String tag, String recallName) {
        List<String> videoIds;

        try {
            ZrevRangeStrategyCommand tagVideosCommand = new ZrevRangeStrategyCommand(getRedisKey(prefix, tag), 0, VIDEO_CUTDOWN);
            videoIds = tagVideosCommand.execute();
        } catch (Exception e) {
            e.printStackTrace();
            return Collections.EMPTY_LIST;
        }

        return MXDataSource.details().get(videoIds, recallName);
    }

    private List<BaseDocument> getVideosFromRedis(String prefix, String tag) {
        List<String> videoIds;

        try {
            ZrevRangeStrategyCommand tagVideosCommand = new ZrevRangeStrategyCommand(getRedisKey(prefix, tag), 0, VIDEO_CUTDOWN);
            videoIds = tagVideosCommand.execute();
        } catch (Exception e) {
            e.printStackTrace();
            return Collections.EMPTY_LIST;
        }

        return MXDataSource.details().get(videoIds);
    }

    /**
     * @param tag
     * @param mode 0: by order; 1: random; 2: mixture (1和2尚未实现)
     * @return List<BaseDocument>
     */

    public List<BaseDocument> getVideosByTag(String prefix, String tag, int mode, String recallName) {
        List<BaseDocument> videos = tagCacheInOrder.get(prefix+tag);

        if (null == videos) {
            videos = getVideosFromRedis(prefix, tag, recallName);
            tagCacheInOrder.putIfAbsent(prefix+tag, videos);
        } else {
            videos.forEach(doc -> doc.recallName = recallName);
        }

        return new ArrayList<>(videos);
    }

    /**
     * @param tag
     * @param mode 0: by order; 1: random; 2: mixture (1和2尚未实现)
     * @return List<BaseDocument>
     */

    public List<BaseDocument> getVideosByTag(String prefix, String tag, int mode) {
        List<BaseDocument> videos = tagCacheInOrder.get(prefix+tag);

        if (null == videos) {
            videos = getVideosFromRedis(prefix, tag);
            tagCacheInOrder.putIfAbsent(prefix+tag, videos);
        }

        return new ArrayList<>(videos);
    }

    @Trace(dispatcher = true)
    public List<BaseDocument> getVideosByTag(String prefix, String tag, String recallName) {
        return getVideosByTag(prefix, tag, 0, recallName);
    }

    @Trace(dispatcher = true)
    public List<BaseDocument> getVideosByTag(String prefix, String tag) {
        return getVideosByTag(prefix, tag, 0);
    }

    private void printVideos(List<BaseDocument> videos) {
        if (null == videos) {
            return;
        }
        int itr = 0;
        for (BaseDocument doc : videos) {
            if (10 < itr) {
                break;
            }
            log.info("Doc " + itr + " : " + doc.id);
        }
    }
}
