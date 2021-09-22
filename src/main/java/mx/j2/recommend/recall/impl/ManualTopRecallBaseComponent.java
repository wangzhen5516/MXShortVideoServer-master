package mx.j2.recommend.recall.impl;

import mx.j2.recommend.component.configurable.config.RecallConfig;
import mx.j2.recommend.component.list.skip.ISkip;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;

import java.util.List;
import java.util.Map;

/**
 * @author xiang.zhou
 * @ Author     ：xiang.zhou
 * @ Date       ：Created in 下午4:05 2020/6/18
 * @ Description：使用可配置扩展后的ManualTopRecall
 */
public class ManualTopRecallBaseComponent extends ManualTopRecall {

    private final static String KEY_REDIS = "redis";


    @Override
    public void registerConfig(Map<String, Class> outConfMap) {
        outConfMap.put(RecallConfig.KEY_SKIP, ISkip.class);
        outConfMap.put(KEY_REDIS, String.class);
    }

    @Override
    public String getRedisKey(BaseDataCollection dc) {
        return config.getString(KEY_REDIS);
    }

    @Override
    public List<BaseDocument> getList(BaseDataCollection dc) {
        return dc.manualList;
    }

    @Override
    public int getRecallSize() {
        return 900;
    }
}