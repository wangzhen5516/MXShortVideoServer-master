package mx.j2.recommend.ranker.impl;

import com.newrelic.api.agent.Trace;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.util.MXJudgeUtils;

/**
 * @author zhongrenli
 */
public class OriginalAudioRanker extends BaseRanker<BaseDataCollection> {

    @Override
    public boolean skip(BaseDataCollection data) {
        return false;
    }

    @Override
    @Trace(dispatcher = true)
    public void rank(BaseDataCollection dc) {
        if (MXJudgeUtils.isEmpty(dc.mergedList)) {
            return;
        }

        dc.mergedList.sort((doc1, doc2) -> Float.compare(doc2.isOriginalAudio, doc1.isOriginalAudio));
    }
}
