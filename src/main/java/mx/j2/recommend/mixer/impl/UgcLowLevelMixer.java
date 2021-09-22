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

import java.util.*;

/**
 * @author ：zhongrenli
 * @date ：Created in 4:52 下午 2020/8/13
 */
public class UgcLowLevelMixer extends BaseMixer<BaseDataCollection> {

    @Override
    public boolean skip(BaseDataCollection data) {
        if (MXJudgeUtils.isEmpty(data.poolToDocumentListMap)) {
            System.out.println("poolToDocumentListMap is Empty!");
            return true;
        }

        return MXStringUtils.isEmpty(data.req.nextToken) || isPureNewUser(data);
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

            List<BaseDocument> toAdd = new ArrayList<>();
            PoolConf conf = entry.getValue();

            // 非低级池子跳过
            if (!conf.poolLevel.contains(DefineTool.EsPoolLevel.LOW.getLevel())) {
                continue;
            }

            double num = conf.percentage;
            moveToList(dc, toAdd, num, documentList);
            for(BaseDocument doc : toAdd){
                doc.setPoolLevel(BaseMagicValueEnum.LOW_LEVEL);
                doc.setPoolIndex(conf.poolIndex);
            }
            addDocsToMixDocument(dc, toAdd);
        }
    }
}
