package mx.j2.recommend.recall.impl;

import com.newrelic.api.agent.Trace;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.hystrix.redis.ZrevRangePvCommand;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXJudgeUtils;
import mx.j2.recommend.util.MXStringUtils;

import java.util.Comparator;
import java.util.List;

/**
 * @author qiqi
 * @date 2021-01-20 17:17
 */
public class RealTimeVideoFromRedisRecall extends BaseRecall<BaseDataCollection> {

    protected static final int CUT_DOWN = 500;

    @Override
    public boolean skip(BaseDataCollection dc) {
        if (dc.req == null || dc.req.userInfo == null) {
            return true;
        }
        String uuId = dc.req.userInfo.getUuid();
        if (MXStringUtils.isBlank(uuId)) {
            return true;
        }
        return false;
    }

    public String get_key_format() {
        return "%s:similar_video_v1";
    }

    public String get_redis_key(BaseDataCollection dc) {
        return String.format(get_key_format(), dc.req.userInfo.getUuid());
    }

    public int getLength() {
        return CUT_DOWN;
    }

    @Override
    @Trace(dispatcher = true)
    public void recall(BaseDataCollection dc) {
        ZrevRangePvCommand pvCommand = new ZrevRangePvCommand(get_redis_key(dc), 0, getLength());

        //TODO
        long startTime = System.nanoTime();
        List<String> videoIds = pvCommand.execute();
        dc.appendToTimeRecord(System.nanoTime()-startTime,this.getName()+"_PvCommand_execute");

        if (MXJudgeUtils.isEmpty(videoIds)) {
            return;
        }

        //TODO
        startTime = System.nanoTime();
        List<BaseDocument> docLists = MXDataSource.details().get(videoIds, this.getName());
        dc.appendToTimeRecord(System.nanoTime()-startTime,this.getName()+"_Detail");

        if (MXJudgeUtils.isEmpty(docLists)) {
            return;
        }

        /*按照heat_score2排序*/
        docLists.sort(Comparator.comparing(BaseDocument::getHeatScore2).reversed());
        dc.similarRealList.addAll(docLists);
        dc.syncSearchResultSizeMap.put(this.getName(), docLists.size());
        dc.resultFromMap.put(this.getName(), DefineTool.RecallFrom.REDIS.getName());
    }
}