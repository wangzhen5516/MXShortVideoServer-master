package mx.j2.recommend.mixer.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * @author ：zhongrenli
 * @date ：Created in 4:46 下午 2021/2/2
 */
public class ManualMixer extends BaseMixer<BaseDataCollection> {

    private final static int MIX_NUMBER = 10;

    @Override
    public boolean skip(BaseDataCollection dc) {
        return CollectionUtils.isEmpty(dc.manualList);
    }

    @Override
    public void mix(BaseDataCollection dc) {
        List<BaseDocument> toAdd = new ArrayList<>();
        moveToList(dc, toAdd, MIX_NUMBER, dc.manualList);
        for(BaseDocument doc : toAdd){
            doc.setPoolLevel("first_top");
        }
        addDocsToMixDocument(dc, toAdd);
    }
}
