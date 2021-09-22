package mx.j2.recommend.mixer.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.util.MXJudgeUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * @author liangjie.feng
 * @version 1.0
 * @date 2021/2/4 下午1:00
 * @description
 */
public class SimilarRealMixer1 extends SimilarRealMixer {

    private double NUM = 0.1;

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
