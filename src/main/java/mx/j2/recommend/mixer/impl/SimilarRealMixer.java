package mx.j2.recommend.mixer.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.util.MXJudgeUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * @author qiqi
 * @date 2021-01-21 12:56
 */
public class SimilarRealMixer extends BaseMixer<BaseDataCollection> {

    private static final double NUM = 8.0;

    @Override
    public boolean skip(BaseDataCollection dc) {
        return MXJudgeUtils.isEmpty(dc.similarRealList);
    }

    @Override
    public void mix(BaseDataCollection dc) {
        List<BaseDocument> toAdd = new ArrayList<>();
        moveToListInOrder(dc, toAdd, NUM, dc.similarRealList);
        addDocsToMixDocument(dc, toAdd);
    }
}
