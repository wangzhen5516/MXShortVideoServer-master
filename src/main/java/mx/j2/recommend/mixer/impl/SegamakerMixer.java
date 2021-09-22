package mx.j2.recommend.mixer.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.util.MXJudgeUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * @author ：zhongrenli
 * @date ：Created in 4:52 下午 2020/8/13
 */
public class SegamakerMixer extends BaseMixer<BaseDataCollection> {

    private static final double NUM = 3.0;

    @Override
    public boolean skip(BaseDataCollection dc) {
        return MXJudgeUtils.isEmpty(dc.predictDocumentList);
    }

    @Override
    public void mix(BaseDataCollection dc) {
        List<BaseDocument> toAdd = new ArrayList<>();
        moveToList(dc, toAdd, NUM, dc.predictDocumentList);
        addDocsToMixDocument(dc, toAdd);
    }
}
