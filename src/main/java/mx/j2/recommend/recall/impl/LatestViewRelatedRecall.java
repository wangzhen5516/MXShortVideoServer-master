package mx.j2.recommend.recall.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_source.StrategyCassandraDataSource;
import mx.j2.recommend.hystrix.redis.ZrevRangePvCommand;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXJudgeUtils;
import mx.j2.recommend.util.MXStringUtils;

import java.util.List;

/**
 * 从CA中获取相似的点赞视频（同RealTimeActionVideoRecall一并使用）
 *
 * @author DuoZhao
 */
@Deprecated
public class LatestViewRelatedRecall extends BaseRecall<BaseDataCollection> {
    private static final String REDIS_KEY_FORMAT = "%s:prefer_video_v1";
    private static final int REDIS_END = 0;

    @Override
    public boolean skip(BaseDataCollection data) {
        return false;
    }

    @Override
    public void recall(BaseDataCollection dc) {
        if (MXStringUtils.isEmpty(dc.client.user.uuId)) {
            return;
        }

        ZrevRangePvCommand zrevRangePvCommand = new ZrevRangePvCommand(String.format(REDIS_KEY_FORMAT, dc.client.user.uuId), 0, REDIS_END);
        List<String> videoId = zrevRangePvCommand.execute();

        if (MXJudgeUtils.isEmpty(videoId)) {
            return;
        }

        StrategyCassandraDataSource strategyCassandraDataSource = MXDataSource.strategyCA();
        List<String> videoIdsFromCa = strategyCassandraDataSource.getRealTimeActionVideoIdList(videoId.get(0));

        if (MXJudgeUtils.isEmpty(videoIdsFromCa)) {
            return;
        }
        List<BaseDocument> docList = MXDataSource.details().get(videoIdsFromCa, this.getName());

        if (MXJudgeUtils.isNotEmpty(docList)) {
            docList.removeIf(item -> videoId.get(0).equals(item.id));
            dc.latestViewRelatedDocList.addAll(docList);
            dc.syncSearchResultSizeMap.put(this.getName(), docList.size());
            dc.resultFromMap.put(this.getName(), DefineTool.RecallFrom.ES.getName());
        }
    }
}