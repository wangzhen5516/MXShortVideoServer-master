package mx.j2.recommend.mixer.impl;

import mx.j2.recommend.component.configurable.config.MixerConfig;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.data_collection.info.MXEntityDebugInfo;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.util.MXJudgeUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @param <T> DC 类型
 * @author zhendong.wang
 * @version 1.0
 * @date 2021/2/26 下午4:42
 * @description 列表混入器基类
 */
public abstract class BaseListMixer<T extends BaseDataCollection, R> extends BaseMixer<T> {
    /**
     * 混入一个列表
     */
    void mixOneList(T dc, List<BaseDocument> list) {
        if (MXJudgeUtils.isEmpty(list)) {
            return;
        }

        List<BaseDocument> toAdd = new ArrayList<>();

        if (getType().equals(MixerConfig.TypeEnum.RANDOM)) {
            moveToList(dc, toAdd, getCount(dc) * dc.req.num, list);
        } else {
            moveToListInOrder(dc, toAdd, getCount(dc) * dc.req.num, list);
        }

        addDocsToMixDocument(dc, toAdd);
    }

    /**
     * 混入一个列表
     */
    void mixOneMap(T dc, Map<String, List<BaseDocument>> map) {
        if (MXJudgeUtils.isEmpty(map)) {
            return;
        }

        float mixTotal = getCount(dc);
        float limit = mixTotal / map.keySet().size();
        List<BaseDocument> toAdd = new ArrayList<>();
        map.forEach((k, v) -> {
            moveToListInOrder(dc, toAdd, limit, v);
            toAdd.forEach(d -> {
                MXEntityDebugInfo entityDebugInfo = dc.debug.getDebugInfoByEntityId(d.id);
                if (entityDebugInfo.recall.name.contains("_")) {
                    return;
                }
                entityDebugInfo.recall.name = String.format("%s_%s", entityDebugInfo.recall.name, k);
            });
            addDocsToMixDocument(dc, toAdd);
        });
    }

    /**
     * 从 DC 中拿召回数据
     */
    R getResult(T dc) {
        String key = getResultKey();
        return (R) dc.getResult(key);
    }
}
