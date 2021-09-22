package mx.j2.recommend.mixer.impl;

import mx.j2.recommend.component.configurable.config.MixerConfig;
import mx.j2.recommend.component.list.skip.ISkip;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.util.MXCollectionUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 基于 publisher 混入限制的模板混入器
 */
public class RealTimeActionByPublisherMixer extends ListMixer {
    private static final String KEY_PER_PUBLISHER = "per_pub";// 每个 publisher 的视频最多数量
    private static final String KEY_SIM_PUBLISHER = "sim_pub";// sim publisher 最多混入数量
    private static final String KEY_TOTAL = "total";// 最多收集的视频数量
    private static final String SOURCE_VALUE_SIM_PUB = "sim_pub";
    private static final String SOURCE_VALUE_DEFAULT = "no_source";
    private static final String RESPONSE_NUM = "response_num";

    @Override
    public void registerConfig(Map<String, Class> outConfMap) {
        outConfMap.put(MixerConfig.KEY_SKIP, ISkip.class);
        outConfMap.put(MixerConfig.KEY_RESULT, String.class);
        outConfMap.put(KEY_PER_PUBLISHER, Integer.class);
        outConfMap.put(KEY_SIM_PUBLISHER, Integer.class);
        outConfMap.put(KEY_TOTAL, Integer.class);
        outConfMap.put(RESPONSE_NUM, Integer.class);
    }

    @Override
    public void mix(BaseDataCollection dc) {
        List<BaseDocument> docList = new ArrayList<>();
        Map<String, Integer> map = new HashMap<>();
        final int PER_PUB_COUNT = getPerPublisherConfig();
        final int SIM_PUB_COUNT = getSimPublisherConfig();
        final int TOTAL_COUNT = getTotalConfig();

        for (BaseDocument doc : getResult(dc)) {
            if (map.getOrDefault(doc.publisher_id, 0) >= PER_PUB_COUNT ||
                    dc.publisherIdSourceMap.getOrDefault(doc.id, SOURCE_VALUE_DEFAULT).equals(SOURCE_VALUE_SIM_PUB)
                            && map.getOrDefault(doc.publisher_id, 0) >= SIM_PUB_COUNT) {
                continue;
            }

            docList.add(doc);

            map.put(doc.publisher_id, map.getOrDefault(doc.publisher_id, 0) + 1);

            if (docList.size() >= TOTAL_COUNT) {
                break;
            }
        }

        if (MXCollectionUtils.isEmpty(docList)) {
            return;
        }

        List<BaseDocument> result = new ArrayList<>();
        if (dc.req.extraClientInfo != null && dc.req.extraClientInfo.isSupportBatchData) {
            List<BaseDocument> mix = docList.size() > getResponseNum() ? docList.subList(0, getResponseNum()) : docList;
            addDocsToMixDocument(dc, mix);
            if (docList.size() > getResponseNum()) {
                result = docList.subList(getResponseNum(), docList.size());
            }
        } else {
            addOneDocToMixDocument(dc, docList.get(0));
            result = new ArrayList<>(docList.subList(1, docList.size()));
        }
        // 剩下的暂存起来，作为后面存储用
        dc.setResult(ListEnum.REAL_TIME_ACTION_STORAGE.value, result);

    }

    /**
     * 获取"每个 publisher 最多混入数量"配置
     */
    private int getPerPublisherConfig() {
        return config.getInt(KEY_PER_PUBLISHER);
    }

    /**
     * 获取"sim publisher 最多混入数量"配置
     */
    private int getSimPublisherConfig() {
        return config.getInt(KEY_SIM_PUBLISHER);
    }

    /**
     * 获取"最多收集视频数量"配置
     */
    private int getTotalConfig() {
        return config.getInt(KEY_TOTAL);
    }

    /**
     * 获取实时responseNum
     *
     * @return
     */
    private int getResponseNum() {
        return config.getInt(RESPONSE_NUM);
    }

}
