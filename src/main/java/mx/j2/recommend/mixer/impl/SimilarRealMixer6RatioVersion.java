package mx.j2.recommend.mixer.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;

import java.util.ArrayList;
import java.util.List;

public class SimilarRealMixer6RatioVersion extends SimilarRealMixer {

    private double RATIO = 0.2;

    @Override
    public void mix(BaseDataCollection dc) {
        double numberToMix = RATIO * (double)dc.req.num;
        List<BaseDocument> toAdd = new ArrayList<>();
        moveToListInOrder(dc, toAdd, numberToMix, dc.similarRealList);
        addDocsToMixDocument(dc, toAdd);
    }
}
