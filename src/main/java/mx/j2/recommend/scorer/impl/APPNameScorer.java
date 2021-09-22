package mx.j2.recommend.scorer.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.util.MXJudgeUtils;

import java.util.HashMap;
import java.util.Map;

public class APPNameScorer extends BaseScorer<BaseDataCollection> {
    private static final Map<String, Integer> APP_NAME_SCORE_MAP;
    static {
        APP_NAME_SCORE_MAP = new HashMap<>();
        APP_NAME_SCORE_MAP.put("roposo", 5);
        APP_NAME_SCORE_MAP.put("vigo", 5);
        APP_NAME_SCORE_MAP.put("vid_status", 5);
    }

    @Override
    public boolean skip(BaseDataCollection dc) {
        if (MXJudgeUtils.isEmpty(dc.mergedList)) {
            return true;
        }
        return false;
    }

    @Override
    public void score(BaseDataCollection dc) {
        for (BaseDocument bdoc : dc.mergedList) {
            if(APP_NAME_SCORE_MAP.containsKey(bdoc.appName.toLowerCase())){
                bdoc.scoreDocument.appNameScore = APP_NAME_SCORE_MAP.get(bdoc.appName.toLowerCase());
            }else {
                bdoc.scoreDocument.appNameScore = 0;
            }
        }
    }
}
