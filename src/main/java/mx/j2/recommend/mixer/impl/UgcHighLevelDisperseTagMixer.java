package mx.j2.recommend.mixer.impl;

import com.newrelic.api.agent.Trace;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.pool_conf.PoolConf;
import mx.j2.recommend.util.BaseMagicValueEnum;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXJudgeUtils;

import java.util.*;

/**
 * @author ：zhongrenli
 * @date ：Created in 4:52 下午 2020/8/13
 */
public class UgcHighLevelDisperseTagMixer extends BaseMixer<BaseDataCollection> {
    @Override
    public boolean skip(BaseDataCollection data) {
        if (MXJudgeUtils.isEmpty(data.poolToDocumentListMap)) {
            return true;
        }

        return resultIsEnough(data);
    }

    @Override
    @Trace(dispatcher = true)
    public void mix(BaseDataCollection dc) {
        Map<String, PoolConf> map = new LinkedHashMap<>();
        Map<String, Map<String, PoolConf>> levelToMap = MXDataSource.pools().all();
        for (Map<String, PoolConf> item : levelToMap.values()) {
            PoolConf pc = item.getOrDefault(dc.recommendFlow.name, item.get("base"));
            if (pc == null) {// 正常情况下不应该走这一步
                continue;
            }
            map.put(pc.poolIndex, pc);
        }
        if (isPureNewUser(dc)) {
            processNewUser(dc, map);
        } else {
            processOldUser(dc, map);
        }


    }

    private void processNewUser(BaseDataCollection dc, Map<String, PoolConf> map) {
        Map<String, List<BaseDocument>> primaryTagDocumentMap = new HashMap<>();
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
                doc.setPoolLevel(BaseMagicValueEnum.HIGH_LEVEL);
                doc.setPoolIndex(conf.poolIndex);
                doc.setPoolPriority(conf.priority);
                String tag = doc.primaryTags == null || doc.primaryTags.isEmpty() ? "no_tag" : doc.primaryTags.getString(0);
                List<BaseDocument> tagDocList = primaryTagDocumentMap.getOrDefault(tag, new ArrayList<>());
                tagDocList.add(doc);
                primaryTagDocumentMap.put(tag, tagDocList);

            }
        }

        List<List<BaseDocument>> docListList = new ArrayList<>();
        for (String key : primaryTagDocumentMap.keySet()) {
            docListList.add(primaryTagDocumentMap.get(key));
        }

        int index = 0;
        //预防死循环
        while (index < 100) {
            if (MXJudgeUtils.isEmpty(docListList)) {
                break;
            }
            docListList.sort((o1, o2) -> Integer.compare(o2.get(0).getPoolPriority(), o1.get(0).getPoolPriority()));

            for (int i = 0; i < docListList.size(); ) {
                List<BaseDocument> list = docListList.get(i);
                addOneDocToMixDocument(dc, list.get(0));
                list.remove(0);
                if (MXJudgeUtils.isEmpty(list)) {
                    docListList.remove(i);
                } else {
                    docListList.set(i, list);
                    i++;
                }
            }
            if (resultIsEnough(dc)) {
                break;
            }
            index++;
        }
    }

    private void processOldUser(BaseDataCollection dc, Map<String, PoolConf> map) {
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
                if (doc.recallName.startsWith("PoolRecall")) {
                    doc.setPoolLevel(BaseMagicValueEnum.HIGH_LEVEL);
                    doc.setPoolIndex(conf.poolIndex);
                } else if (doc.recallName.startsWith("SingleToPool11Recall")) {
                    doc.setPoolLevel(BaseMagicValueEnum.HIGH_LEVEL);
                    doc.setPoolIndex(conf.poolIndex);
                } else if (doc.recallName.startsWith("TagRecall")) {
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
