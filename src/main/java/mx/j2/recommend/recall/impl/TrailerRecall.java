package mx.j2.recommend.recall.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.MXStringUtils;

import java.util.Collections;
import java.util.List;

@Deprecated
public class TrailerRecall extends BaseRecall<BaseDataCollection> {
    private static final String TRAILER_ID = "f10d0c4966007ce2728c925871e020d5";

    @Override
    public boolean skip(BaseDataCollection data) {
        return false;
    }

    @Override
    public void recall(BaseDataCollection dc) {
        if (MXStringUtils.isNotEmpty(dc.req.nextToken)) {
            return;
        }

        List<BaseDocument> doc = MXDataSource.details().get(Collections.singleton(TRAILER_ID), this.getName());

        if (null == doc || doc.size() != 1) {
            return;
        }

        dc.trailerVideo.addAll(doc);
    }
}
