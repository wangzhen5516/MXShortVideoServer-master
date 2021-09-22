package mx.j2.recommend.recall.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;

import mx.j2.recommend.util.DefineTool;

import mx.j2.recommend.util.MXJudgeUtils;

import java.util.HashMap;

import java.util.Map;

/**
 * @author : Qi Mao
 * @date: 12/23/2020
 */

public class EffectVideoRecall extends SearchEngineRecall<BaseDataCollection> {

    //由于数据那边导入有问题，部分相同effect有两个effect_id，需要归并
    private static Map<String, String> AGG_MAP = new HashMap<String, String>();

    public EffectVideoRecall() {
        init();
    }
    public void init(){
        AGG_MAP.put("20397", "1612543181173");
        AGG_MAP.put("1612543181173", "20397");

        AGG_MAP.put("20396", "1612543302056");
        AGG_MAP.put("1612543302056", "20396");

        AGG_MAP.put("20395", "1612543403616");
        AGG_MAP.put("1612543403616", "20395");

        AGG_MAP.put("20394", "1612441009142");
        AGG_MAP.put("1612441009142", "20394");

        AGG_MAP.put("20393", "1612438268344");
        AGG_MAP.put("1612438268344", "20397");
    }

    @Override
    public void recall(BaseDataCollection dc) {
        constructRequestURL(dc);
    }

    @Override
    public boolean skip(BaseDataCollection dc) {
        if (MXJudgeUtils.isEmpty(dc.req.resourceType) || MXJudgeUtils.isEmpty(dc.req.resourceId)
                || !"effect".equals(dc.req.resourceType)) {
            return true;
        }
        return false;
    }

    @Override
    public void constructRequestURL(BaseDataCollection dc) {
        String esContent;

        String HEAD_ES_CONTENT_FORMAT = "{\"from\":0,\"size\":%s,\"query\":{\"bool\":{\"must\":[{\"match\":{\"effect_type\":\"%s\"}},{\"match\":{\"status\":\"1\"}}]}},\"sort\":[{\"heat_score2\":{\"order\":\"desc\",\"missing\":0}},{\"online_time\":{\"order\":\"desc\"}}]}";

        String NEXT_ES_CONTENT_FORMAT = "{\"from\":0,\"size\":%s,\"query\":{\"bool\":{\"must\":[{\"match\":{\"effect_type\":\"%s\"}},{\"match\":{\"status\":\"1\"}}]}},\"search_after\":[\"%s\",\"%s\"],\"sort\":[{\"heat_score2\":{\"order\":\"desc\",\"missing\":0}},{\"online_time\":{\"order\":\"desc\"}}]}";

        if (MXJudgeUtils.isNotEmpty(dc.req.nextToken)) {
            String[] tokens = dc.req.nextToken.split("\\|");
            if (tokens.length < 2) {
                return;
            }
            int size = dc.req.num == 0 ? 40 : dc.req.num * 3;

            StringBuilder sb = new StringBuilder(dc.req.resourceId);

            if (AGG_MAP.containsKey(dc.req.resourceId)) {
                sb.append("\",\"");
                sb.append(AGG_MAP.get(dc.req.resourceId));
            }
            esContent = String.format(NEXT_ES_CONTENT_FORMAT, size, sb.toString(), tokens[0], tokens[1]);

        } else {
            int size = dc.req.num == 0 ? 40 : dc.req.num * 3;

            StringBuilder sb = new StringBuilder(dc.req.resourceId);

            if (AGG_MAP.containsKey(dc.req.resourceId)) {
                sb.append("\",\"");
                sb.append(AGG_MAP.get(dc.req.resourceId));
            }
            esContent = String.format(HEAD_ES_CONTENT_FORMAT, size, sb.toString());

        }
        if (esContent == null) {
            return;
        }

        String indexUrl = DefineTool.CategoryEnum.SHORT_VIDEO.getIndexAndType();
        String elasticSearchRequest = String.format(requestUrlFormat, indexUrl);

        dc.addToESRequestList(
                elasticSearchRequest,
                esContent,
                this.getName(), "",
                DefineTool.EsType.VIDEO.getTypeName()
        );
        dc.resultFromMap.put(this.getName(), DefineTool.RecallFrom.ES.getName());
        dc.searchEngineRecallerSet.add(this.getName());
    }
}
