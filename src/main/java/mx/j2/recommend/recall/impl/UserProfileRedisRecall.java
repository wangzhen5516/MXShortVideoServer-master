package mx.j2.recommend.recall.impl;

import com.newrelic.api.agent.Trace;
import mx.j2.recommend.component.configurable.config.RecallConfig;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.hystrix.redis.ZrevRangePvCommand;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXJudgeUtils;

import java.util.List;
import java.util.Map;

/**
 * redis 个性化数据召回器
 */
@SuppressWarnings("unused")
public class UserProfileRedisRecall extends BaseRecall<BaseDataCollection> {
    private static final String KEY_REDIS_KEY_SUFFIX = "redis_key_suffix";

    @Override
    @Trace(dispatcher = true)
    public void recall(BaseDataCollection dc) {
        ZrevRangePvCommand pvCommand = new ZrevRangePvCommand(getRedisKey(dc), 0, getSize());

        //TODO
        long startTime = System.nanoTime();
        List<String> videoIds = pvCommand.execute();
        dc.appendToTimeRecord(System.nanoTime() - startTime, this.getName() + "_PvCommand_execute");

        if (MXJudgeUtils.isEmpty(videoIds)) {
            return;
        }

        //TODO
        startTime = System.nanoTime();
        List<BaseDocument> docLists = MXDataSource.details().get(videoIds);
        dc.appendToTimeRecord(System.nanoTime() - startTime, this.getName() + "_Detail");

        if (MXJudgeUtils.isEmpty(docLists)) {
            return;
        }

        addResult(dc, docLists);

        dc.syncSearchResultSizeMap.put(this.getName(), docLists.size());
        dc.resultFromMap.put(this.getName(), DefineTool.RecallFrom.REDIS.getName());
    }

    @Override
    public void registerConfig(Map<String, Class> outConfMap) {
        super.registerConfig(outConfMap);
        outConfMap.put(RecallConfig.KEY_SIZE, Integer.class);
        outConfMap.put(KEY_REDIS_KEY_SUFFIX, String.class);
    }

    private String getRedisKey(BaseDataCollection dc) {
        return DefineTool.toKey(dc.req.userInfo.getUuid(), getRedisKeySuffix());
    }

    private String getRedisKeySuffix() {
        return config.getString(KEY_REDIS_KEY_SUFFIX);
    }
}