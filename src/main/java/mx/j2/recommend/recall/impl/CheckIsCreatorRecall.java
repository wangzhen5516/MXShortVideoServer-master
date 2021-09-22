package mx.j2.recommend.recall.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.MXJudgeUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CheckIsCreatorRecall extends BaseRecall<BaseDataCollection> {
    private final Logger logger = LogManager.getLogger(CheckIsCreatorRecall.class);

    private final String RESOURCE_TYPE = "checkCreator";
    private final int DAY_IN_SECOND = 24 * 3600;
    // 与中央标准时间的时差
    private final int UTC_TO_IST = 5 * 3600 + 1800;

    @Override
    public void recall(BaseDataCollection dc) {
        if (MXJudgeUtils.isEmpty(dc.req.resourceType) || !dc.req.resourceType.equals(RESOURCE_TYPE)) {
            return;
        }

        if (MXJudgeUtils.isEmpty(dc.req.resourceId)) {
            return;
        }

        int days;
        try {
            days = Integer.parseInt(dc.req.resourceId);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e.toString());
            return;
        }

        if (days < 1) {
            return;
        }

        Long currentTime = System.currentTimeMillis();
        Long limitTime = ((currentTime / 1000 / DAY_IN_SECOND) * DAY_IN_SECOND - UTC_TO_IST - days * DAY_IN_SECOND) * 1000;
        int uploadDays = MXDataSource.creator().checkCreator(dc.client.user.userId, limitTime);

        dc.uploadDaysRecent = uploadDays;
    }
}

