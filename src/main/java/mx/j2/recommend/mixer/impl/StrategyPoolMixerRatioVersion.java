package mx.j2.recommend.mixer.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.pool_conf.StrategyPoolConf;

import java.util.ArrayList;
import java.util.List;

public class StrategyPoolMixerRatioVersion extends BaseMixer<BaseDataCollection> {
    @Override
    public void mix(BaseDataCollection dc) {
        dc.strategyPoolToDocumentListMap.forEach((k, v) -> {
            if (!dc.strategyPoolConfMap.containsKey(k)) {
                return;
            }
            StrategyPoolConf conf = dc.strategyPoolConfMap.get(k);
            double basePercentage = conf.basePercentage;
            if (null != conf.smallFlowMap && conf.smallFlowMap.containsKey(dc.recommendFlow.name)) {
                basePercentage = conf.smallFlowMap.get(dc.recommendFlow.name).percentage;
            }
            basePercentage = (basePercentage / 15) * dc.req.num;
            List<BaseDocument> toAdd = new ArrayList<>();
            moveToList(dc, toAdd, basePercentage, v);
            for (BaseDocument doc : toAdd) {
                doc.setPoolLevel(k);
                doc.setPoolIndex(conf.poolIndexPrefix);
            }
            addDocsToMixDocument(dc, toAdd);
        });
    }

    @Override
    public boolean skip(BaseDataCollection dc) {
        return dc.strategyPoolToDocumentListMap.isEmpty();
    }
}
