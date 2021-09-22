package mx.j2.recommend.mixer.impl;

import com.newrelic.api.agent.Trace;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_source.UgcLowLevelMixParamDataSource;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.pool_conf.PoolConf;
import mx.j2.recommend.util.BaseMagicValueEnum;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXJudgeUtils;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * @author ：zhongrenli
 * @date ：Created in 6:32 下午 2021/6/19
 */
public class UgcLowLevelMixerRatioVersionForMain extends BaseMixer<BaseDataCollection>{
    @Override
    public boolean skip(BaseDataCollection data) {
        if (MXJudgeUtils.isEmpty(data.poolToDocumentListMap)) {
            System.out.println("poolToDocumentListMap is Empty!");
            return true;
        }

        return false;
    }

    @Override
    @Trace(dispatcher = true)
    public void mix(BaseDataCollection dc) {
        Map<String, PoolConf> map = new LinkedHashMap<>();
        Map<String, Map<String, PoolConf>> levelToMap =  MXDataSource.pools().all();
        for (Map<String, PoolConf> item : levelToMap.values()) {
            PoolConf pc = item.getOrDefault(dc.recommendFlow.name, item.get("base"));
            if(pc == null) {
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

            List<BaseDocument> toAdd = new ArrayList<>();
            PoolConf conf = entry.getValue();

            // 非低级池子跳过
            if (!conf.poolLevel.contains(DefineTool.EsPoolLevel.LOW.getLevel())) {
                continue;
            }

            double ratio = conf.percentage;
            ratio = (documentList.size() >= UgcLowLevelMixParamDataSource.getMixParam()) ? ratio : (documentList.size() / UgcLowLevelMixParamDataSource.getMixParam() * ratio);
            double numberToMixer = ratio * (double) dc.req.num;

            moveToList(dc, toAdd, numberToMixer, documentList);
            for(BaseDocument doc : toAdd){
                doc.setPoolLevel(BaseMagicValueEnum.LOW_LEVEL);
                doc.setPoolIndex(conf.poolIndex);
            }

            addDocsToMixDocument(dc, toAdd);
        }
    }
}
