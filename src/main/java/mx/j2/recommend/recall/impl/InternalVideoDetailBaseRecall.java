package mx.j2.recommend.recall.impl;

import mx.j2.recommend.data_model.data_collection.InternalDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.MXJudgeUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class InternalVideoDetailBaseRecall extends InternalBaseRecall {
    private static Logger logger = LogManager.getLogger(InternalVideoDetailBaseRecall.class);

    @Override
    public void recall(InternalDataCollection dc) {
        if (MXJudgeUtils.isEmpty(dc.internalReq.resourceIdList)) {
            return;
        }

        List<BaseDocument> docList = MXDataSource.details().get(dc.internalReq.resourceIdList, this.getName());

        dc.mergedList.addAll(docList);
    }
}
