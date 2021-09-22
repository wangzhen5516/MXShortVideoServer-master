package mx.j2.recommend.mixer.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.recall.impl.RealTimePubRecall;
import mx.j2.recommend.util.MXCollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

public class RealTimePublisherMixer extends BaseMixer<BaseDataCollection>{
    private static final double mixNum = 4.0;
    @Override
    public void mix(BaseDataCollection dc) {
        if(MXCollectionUtils.isEmpty(dc.userPrePubDocList)){
            return;
        }
        List<BaseDocument> toAdd = new ArrayList<>();
        moveToList(dc,toAdd,mixNum,dc.userPrePubDocList);
        addDocsToMixDocument(dc,toAdd);
    }
}
