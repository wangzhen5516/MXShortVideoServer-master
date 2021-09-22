package mx.j2.recommend.recall.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;

import java.util.List;

/**
 * @author zhongrenli
 * @ Author     ：zhongrenli
 * @ Date       ：Created in 下午4:05 2018/12/5
 * @ Description：${description}
 */
public class ManualTopRecall2 extends ManualTopRecall {

    private final static String REDIS_KEY = "manual_top_funny_videos_v1";

    @Override
    public String getRedisKey(BaseDataCollection dc) {
        return REDIS_KEY;
    }

    @Override
    public List<BaseDocument> getList(BaseDataCollection dc) {
        return dc.manualList;
    }
}