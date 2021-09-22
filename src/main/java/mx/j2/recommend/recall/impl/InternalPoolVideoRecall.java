package mx.j2.recommend.recall.impl;

import mx.j2.recommend.data_model.data_collection.InternalDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.MXJudgeUtils;
import mx.j2.recommend.util.MXStringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class InternalPoolVideoRecall extends InternalBaseRecall {
    private static Logger logger = LogManager.getLogger(InternalPoolVideoRecall.class);

    @Override
    public void recall(InternalDataCollection dc) {
        if (MXJudgeUtils.isEmpty(dc.internalReq.resourceIdList)) {
            return;
        }

        List<BaseDocument> docList = MXDataSource.details().get(dc.internalReq.resourceIdList, this.getName());

        String[] additionalInfo = MXStringUtils.split(dc.internalReq.additionalInfo, "|");
        if (MXJudgeUtils.isNotEmpty(docList)) {
            if (additionalInfo.length == 2) {
                docList.get(0).setPoolPriority(Integer.parseInt(additionalInfo[0]));
                docList.get(0).setPoolLevel(additionalInfo[1]);
            }
            docList.get(0).setRecallName("PoolRecall");
            dc.mergedList.add(docList.get(0));
        }
    }
}
