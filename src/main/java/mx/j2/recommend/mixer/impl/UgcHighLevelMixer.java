package mx.j2.recommend.mixer.impl;

import com.newrelic.api.agent.Trace;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.pool_conf.PoolConf;
import mx.j2.recommend.util.BaseMagicValueEnum;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXJudgeUtils;
import mx.j2.recommend.util.MXStringUtils;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * @author ：zhongrenli
 * @date ：Created in 4:52 下午 2020/8/13
 */
public class UgcHighLevelMixer extends BaseMixer<BaseDataCollection> {

    @Override
    public boolean skip(BaseDataCollection data) {
        if (MXJudgeUtils.isEmpty(data.poolToDocumentListMap)) {
            System.out.println("poolToDocumentListMap is Empty!");
            return true;
        }

        return resultIsEnough(data);
    }

    @Override
    @Trace(dispatcher = true)
    public void mix(BaseDataCollection dc) {
        Map<String, PoolConf> map = new LinkedHashMap<>();
        Map<String, Map<String, PoolConf>> levelToMap =  MXDataSource.pools().all();
        for (Map<String, PoolConf> item : levelToMap.values()) {
            PoolConf pc = item.getOrDefault(dc.recommendFlow.name, item.get("base"));
            if(pc == null) {// 正常情况下不应该走这一步
                continue;
            }
            map.put(pc.poolIndex, pc);
        }

        for (Map.Entry<String, PoolConf> entry : map.entrySet()) {
            String poolIndex = entry.getKey();
            if (!dc.poolToDocumentListMap.containsKey(poolIndex)) {
                continue;
            }
            List<BaseDocument> documentList = dc.poolToDocumentListMap.get(poolIndex);
            if (MXJudgeUtils.isEmpty(documentList)) {
                continue;
            }
            PoolConf conf = entry.getValue();

            // 非高级池子跳过
            if (!conf.poolLevel.contains(DefineTool.EsPoolLevel.HIGH.getLevel())) {
                continue;
            }

            if (Double.compare(conf.percentage, 0) <= 0) {
                continue;
            }

            for (BaseDocument doc : documentList) {
                if(doc.recallName.startsWith("PoolRecall")) {
                    doc.setPoolLevel(BaseMagicValueEnum.HIGH_LEVEL);
                    doc.setPoolIndex(conf.poolIndex);
                } else if(doc.recallName.startsWith("SingleToPool11Recall")){
                    doc.setPoolLevel(BaseMagicValueEnum.HIGH_LEVEL);
                    doc.setPoolIndex(conf.poolIndex);
                } else if(doc.recallName.startsWith("TagRecall")){
                    doc.setPoolLevel(BaseMagicValueEnum.HIGH_LEVEL);
                    doc.setPoolIndex(conf.poolIndex);
                }
                addOneDocToMixDocument(dc, doc);

                if (resultIsEnough(dc)) {
                    break;
                }
            }

            if (resultIsEnough(dc)) {
                break;
            }
        }
    }

}
