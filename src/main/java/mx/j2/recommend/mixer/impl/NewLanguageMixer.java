package mx.j2.recommend.mixer.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;

import java.util.ArrayList;
import java.util.List;

/**
 * @author ：zhongrenli
 * @date ：Created in 4:52 下午 2020/8/13
 */
public class NewLanguageMixer extends BaseMixer<BaseDataCollection> {

    @Override
    public boolean skip(BaseDataCollection data) {
        return false;
    }

    @Override
    public void mix(BaseDataCollection dc) {
        double num = 1.0;

        List<BaseDocument> toAdd = new ArrayList<>();
        moveToList(dc, toAdd, num, dc.newLanguageDocumentList);
        addDocsToMixDocument(dc, toAdd);
    }
}
