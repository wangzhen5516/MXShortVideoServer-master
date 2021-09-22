package mx.j2.recommend.mixer.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.pool_conf.StrategyPoolConf;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RealTimeStrategyPoolMixer extends BaseMixer<BaseDataCollection>{
    double RATIO = 0.2;

    @Override
    public boolean skip(BaseDataCollection dc) {
        return dc.realTimeStrategyPoolToDocumentListMap.isEmpty();
    }

    @Override
    public void mix(BaseDataCollection dc) {
        int mixCount = 0;

        for (Map.Entry<String, List<BaseDocument>> entry : dc.realTimeStrategyPoolToDocumentListMap.entrySet()) {
            if (!dc.realTimeStrategyPoolConfMap.containsKey(entry.getKey())) {
                return;
            }
            StrategyPoolConf conf = dc.realTimeStrategyPoolConfMap.get(entry.getKey());
            double basePercentage = conf.basePercentage;
            if (null != conf.smallFlowMap && conf.smallFlowMap.containsKey(dc.recommendFlow.name)) {
                basePercentage = conf.smallFlowMap.get(dc.recommendFlow.name).percentage;
            }
            List<BaseDocument> toAdd = new ArrayList<>();
            moveToList(dc, toAdd, basePercentage, entry.getValue());
            for (BaseDocument doc : toAdd) {
                doc.setPoolLevel(entry.getKey());
                doc.setPoolIndex(conf.poolIndexPrefix);
            }
            addDocsToMixDocument(dc, toAdd);

            // 如果超出最大数量，跳出
            mixCount += toAdd.size();
            if (mixCount >= RATIO * dc.req.num) {
                break;
            }
        }
    }
}
