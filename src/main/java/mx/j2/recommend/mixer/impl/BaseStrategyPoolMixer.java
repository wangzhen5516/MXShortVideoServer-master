package mx.j2.recommend.mixer.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.pool_conf.StrategyPoolConf;
import mx.j2.recommend.util.MXCollectionUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * @author ：zhongrenli
 * @date ：Created in 4:46 下午 2021/2/2
 */
public abstract class BaseStrategyPoolMixer extends BaseMixer<BaseDataCollection> {
    @Override
    public void mix(BaseDataCollection dc) {
        dc.strategyPoolConfMap.forEach((k, v) -> {
            String tag = k.split("_")[k.split("_").length-1];
            if (check(dc, k, v, tag)) {
                return;
            }

            List<BaseDocument> documents = getDocList(dc, tag);
            if (MXCollectionUtils.isEmpty(documents)) {
                return;
            }

            double basePercentage = v.basePercentage;
            if (null != v.smallFlowMap && v.smallFlowMap.containsKey(dc.recommendFlow.name)) {
                basePercentage = v.smallFlowMap.get(dc.recommendFlow.name).percentage;
            }

            List<BaseDocument> toAdd = new ArrayList<>();
            moveToList(dc, toAdd, basePercentage, documents);
            for(BaseDocument doc : toAdd){
                doc.setPoolLevel(getPoolLevel());
                doc.setPoolIndex(k);
            }
            addDocsToMixDocument(dc, toAdd);
            if (basePercentage >= 1 && basePercentage == toAdd.size()) {
                dc.tagSet.add(tag);
            }
        });
    }

    private List<BaseDocument> getDocList(BaseDataCollection dc, String tag) {
        return dc.strategyTagDocumentListMap.getOrDefault(tag, new ArrayList<>());
    }

    private boolean check(BaseDataCollection dc, String k, StrategyPoolConf v, String tag) {
        return resultIsEnough(dc)
                || !v.poolLevel.contains(getLevel())
                || !dc.strategyPoolToDocumentListMap.containsKey(k)
                || dc.tagSet.contains(tag);
    }

    public String getLevel() {
        return null;
    }

    public String getPoolLevel() {
        return null;
    }

    @Override
    public boolean skip(BaseDataCollection dc) {
        return dc.strategyPoolToDocumentListMap.isEmpty() || dc.strategyTagDocumentListMap.isEmpty();
    }
}
