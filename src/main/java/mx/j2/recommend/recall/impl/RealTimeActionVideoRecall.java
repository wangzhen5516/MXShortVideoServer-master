package mx.j2.recommend.recall.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_source.StrategyCassandraDataSource;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXCollectionUtils;
import mx.j2.recommend.util.MXJudgeUtils;
import mx.j2.recommend.util.MXStringUtils;

import java.util.List;

/**
 * 从CA中获取相似的点赞视频（同LatestViewRelatedRecall一并使用）
 *
 * @author DuoZhao
 */
public class RealTimeActionVideoRecall extends BaseRecall<BaseDataCollection> {

    @Override
    public boolean skip(BaseDataCollection dc) {
        if (null == dc.req.extraClientInfo || MXStringUtils.isEmpty(dc.req.extraClientInfo.lastInteractiveId)) {
            return true;
        }

        List<String> type = dc.recommendFlow.realType;
        if (MXCollectionUtils.isEmpty(type)) {
            return true;
        }
        return !type.contains(dc.req.extraClientInfo.lastInteractiveType);
    }

    @Override
    public void recall(BaseDataCollection dc) {
        StrategyCassandraDataSource strategyCassandraDataSource = MXDataSource.strategyCA();
        List<String> videoIds = strategyCassandraDataSource.getRealTimeActionVideoIdList(dc.req.extraClientInfo.lastInteractiveId);

        if (MXJudgeUtils.isEmpty(videoIds)) {
            return;
        }

        List<BaseDocument> docList = MXDataSource.details().get(videoIds, this.getName());

        if (MXJudgeUtils.isNotEmpty(docList)) {
            docList.removeIf(item -> dc.req.extraClientInfo.lastInteractiveId.equals(item.id));
            dc.realTimeClickDocList.addAll(docList);
            dc.syncSearchResultSizeMap.put(this.getName(), docList.size());
            dc.resultFromMap.put(this.getName(), DefineTool.RecallFrom.CASSANDRA.getName());
        }
    }
}
