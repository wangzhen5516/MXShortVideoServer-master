package mx.j2.recommend.mixer.impl;

import mx.j2.recommend.data_model.data_collection.FeedDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.MXJudgeUtils;

import java.util.ArrayList;
import java.util.List;

public class InterestTagMixer extends BaseMixer<FeedDataCollection> {
    private static final double NUM = 3.0;

    @Override
    public boolean skip(FeedDataCollection dc) {
        return MXJudgeUtils.isEmpty(dc.interestTagDocList);
    }

    @Override
    public void mix(FeedDataCollection dc) {
        List<BaseDocument> toAdd = new ArrayList<>();
        moveToListInOrder(dc, toAdd, NUM, dc.interestTagDocList);
        dc.tagPoolSmallFlow = "Normal";
        addDocsToMixDocument(dc, toAdd);
    }
}
