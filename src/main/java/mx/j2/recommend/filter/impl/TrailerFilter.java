package mx.j2.recommend.filter.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.MXJudgeUtils;

public class TrailerFilter extends BaseFilter {
    private static final String TRAILER_BLOOM_KEY = "f10d0c4966007ce2728c925871e020d5_bloom_trailer";

    @Override
    public boolean isFilted(BaseDocument doc, BaseDataCollection dc) {
        return false;
    }

    @Override
    public boolean prepare(BaseDataCollection dc) {
        if (MXJudgeUtils.isEmpty(dc.trailerVideo)) {
            return true;
        }

        String[] values = {dc.client.user.uuId};
        boolean[] existsRes = MXDataSource.rebloom().exists(TRAILER_BLOOM_KEY, values);

        if (null == existsRes || existsRes.length != 1) {
            dc.trailerVideo.clear();
            return true;
        }

        if (existsRes[0]) {
            dc.trailerVideo.clear();
            return true;
        }

        MXDataSource.rebloom().add(TRAILER_BLOOM_KEY, values);

        return true;
    }
}
