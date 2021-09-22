package mx.j2.recommend.recall.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_source.LocalCacheDataSource;
import mx.j2.recommend.hystrix.redis.ZrevRangePvCommand;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXCollectionUtils;
import mx.j2.recommend.util.MXJudgeUtils;
import mx.j2.recommend.util.MXStringUtils;

import java.util.List;

/**
 * @author liangjie.feng
 * @version 1.0
 * @date 2021/2/6 下午12:01
 * @description
 */
public class RealTimePublisherHeatRecallRedis extends BaseRecall<BaseDataCollection> {

    private final String REDIS_KEY_FORMAT = "item_reco_cf_02-%s";

    @Override
    public boolean skip(BaseDataCollection baseDC) {
        String videoId = null;
        if (baseDC.req.extraClientInfo != null) {
            videoId = baseDC.req.extraClientInfo.lastInteractiveId;
        }
        if (MXStringUtils.isEmpty(videoId)) {
            return true;
        }
        List<String> type = baseDC.recommendFlow.realType;
        if (MXCollectionUtils.isEmpty(type)) {
            return true;
        }
        return type.contains(baseDC.req.extraClientInfo.lastInteractiveType);
    }

    @Override
    public void recall(BaseDataCollection dc) {
        String videoId = dc.req.extraClientInfo.lastInteractiveId;

        LocalCacheDataSource localCacheDataSource = MXDataSource.cache();
        String localCacheKey = String.format("%s_%s", this.getName(), videoId);
        List<BaseDocument> docList = localCacheDataSource.getRealtimePublisherCache(localCacheKey);
        if (null == docList) {
            ZrevRangePvCommand zrevRangePvCommand = new ZrevRangePvCommand(String.format(REDIS_KEY_FORMAT, videoId), 0, 100);
            List<String> idList = zrevRangePvCommand.execute();

            docList = MXDataSource.details().get(idList, this.getName());
            localCacheDataSource.setRealtimePublisherCache(localCacheKey, docList);
        }
        if (MXJudgeUtils.isNotEmpty(docList)) {
            if (null != dc.req.extraClientInfo && MXStringUtils.isNotEmpty(dc.req.extraClientInfo.lastInteractiveId)) {
                docList.removeIf(item -> dc.req.extraClientInfo.lastInteractiveId.equals(item.id));
            }

            dc.realTimeClickDocList.addAll(docList);
            dc.syncSearchResultSizeMap.put(this.getName(), docList.size());
            dc.resultFromMap.put(this.getName(), DefineTool.RecallFrom.REDIS.getName());
        }
    }
}
