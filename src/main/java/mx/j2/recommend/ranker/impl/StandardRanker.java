package mx.j2.recommend.ranker.impl;

import com.newrelic.api.agent.Trace;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.util.MXJudgeUtils;

/**
 * @author zhongrenli
 */
public class StandardRanker extends BaseRanker<BaseDataCollection> {

    @Override
    public boolean skip(BaseDataCollection dc) {
        if (MXJudgeUtils.isEmpty(dc.mergedList)) {
            return true;
        }
        return false;
    }

    @Override
    @Trace(dispatcher = true)
    public void rank(BaseDataCollection dc) {
        dc.mergedList.forEach(doc -> {
            doc.totalScore = doc.scoreDocument.baseScore
                    + doc.scoreDocument.appNameScore
                    + doc.scoreDocument.languageScore
                    + doc.scoreDocument.recallWeightScore
                    + doc.scoreDocument.minusScore
            ;
        });

        dc.mergedList.sort((doc1, doc2) -> Float.compare(doc2.totalScore, doc1.totalScore));
    }
}
