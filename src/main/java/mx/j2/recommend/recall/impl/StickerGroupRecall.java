package mx.j2.recommend.recall.impl;

import com.alibaba.fastjson.JSONObject;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_model.document.StickerGroupDocument;
import mx.j2.recommend.data_source.LocalCacheDataSource;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXJudgeUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * @author liangjie.feng
 * @version 1.0
 * @date 2021/3/25 下午1:45
 * @description
 */
public class StickerGroupRecall extends BaseRecall<BaseDataCollection> {

    private static final String REQUEST_FORMAT = "/%s/_search";

    @Override
    public boolean skip(BaseDataCollection baseDC) {
        return super.skip(baseDC);
    }

    @Override
    public void recall(BaseDataCollection dc) {
        String requestStickerGroup = String.format(REQUEST_FORMAT, DefineTool.CategoryEnum.STICKER_GROUP.getIndexAndType());

        LocalCacheDataSource localCacheDataSource = MXDataSource.cache();
        String localCacheKey = this.getName();
        List<BaseDocument> localCacheResultList = localCacheDataSource.getStickerGroupRecallCache(localCacheKey);
        if (MXJudgeUtils.isNotEmpty(localCacheResultList)) {
            dc.mergedList.addAll(localCacheResultList);
            dc.resultFromMap.put(this.getName(), DefineTool.RecallFrom.LOCAL.getName());
            dc.syncSearchResultSizeMap.put(this.getName(), localCacheResultList.size());
            return;
        }

        List<BaseDocument> esResultList = new ArrayList<>();
        //获取符合条件的stickerGroup
        List<JSONObject> esJsonList = MXDataSource.ES().sendSyncSearchPure(requestStickerGroup, constructQueryBody());

        if (MXJudgeUtils.isEmpty(esJsonList)) {
            return;
        }

        for (JSONObject jsonObject : esJsonList) {
            BaseDocument doc = new StickerGroupDocument().loadJson(jsonObject, DefineTool.CategoryEnum.STICKER_GROUP, this.getName());
            if (doc != null){
                esResultList.add(doc);
            }
        }

        localCacheDataSource.setStickerGroupRecallCache(localCacheKey, esResultList);
        dc.mergedList.addAll(esResultList);
        dc.resultFromMap.put(this.getName(), DefineTool.RecallFrom.ES.getName());
        dc.syncSearchResultSizeMap.put(this.getName(), esResultList.size());
    }

    private String constructQueryBody() {
        return "{\"query\":{\"match\":{\"status\":1}},\"sort\":[{\"order\":{\"order\":\"asc\"}}],\"size\":200}";
    }
}
