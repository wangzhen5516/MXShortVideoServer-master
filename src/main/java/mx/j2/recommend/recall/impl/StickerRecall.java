package mx.j2.recommend.recall.impl;

import com.alibaba.fastjson.JSONObject;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_model.document.StickerDocument;
import mx.j2.recommend.data_source.LocalCacheDataSource;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXJudgeUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * @author liangjie.feng
 * @version 1.0
 * @date 2021/3/29 上午11:15
 * @description
 */
public class StickerRecall extends BaseRecall<BaseDataCollection> {

    private static final String REQUEST_FORMAT = "/%s/_search";

    @Override
    public boolean skip(BaseDataCollection baseDC) {
        return MXJudgeUtils.isEmpty(baseDC.req.resourceId);
    }

    @Override
    public void recall(BaseDataCollection dc) {
        String requestStickerGroup = String.format(REQUEST_FORMAT, DefineTool.CategoryEnum.STICKER.getIndexAndType());

        LocalCacheDataSource localCacheDataSource = MXDataSource.cache();
        String localCacheKey = String.format("%s_%s", this.getName(), dc.req.resourceId);
        List<BaseDocument> localCacheResultList = localCacheDataSource.getStickerRecallCache(localCacheKey);
        if (MXJudgeUtils.isNotEmpty(localCacheResultList)) {
            dc.mergedList.addAll(localCacheResultList);
            dc.resultFromMap.put(this.getName(), DefineTool.RecallFrom.LOCAL.getName());
            dc.syncSearchResultSizeMap.put(this.getName(), localCacheResultList.size());
            return;
        }

        List<BaseDocument> esResultList = new ArrayList<>();
        //根据stickerGroup id获取对应的sticker
        List<JSONObject> esJsonList = MXDataSource.ES().sendSyncSearchPure(requestStickerGroup, String.format(constructQueryBody(), dc.req.resourceId));

        if (MXJudgeUtils.isEmpty(esJsonList)) {
            return;
        }

        for (JSONObject jsonObject : esJsonList) {
            BaseDocument doc = new StickerDocument().loadJson(jsonObject, DefineTool.CategoryEnum.STICKER, this.getName());
            if (doc != null){
                esResultList.add(doc);
            }
        }

        localCacheDataSource.setStickerRecallCache(localCacheKey, esResultList);
        dc.mergedList.addAll(esResultList);
        dc.resultFromMap.put(this.getName(), DefineTool.RecallFrom.ES.getName());
        dc.syncSearchResultSizeMap.put(this.getName(), esResultList.size());
    }

    private String constructQueryBody() {
        return "{\"query\":{\"bool\":{\"must\":[{\"match\":{\"sticker_group\":\"%s\"}},{\"match\":{\"status\":1}}]}}}";
    }
}
