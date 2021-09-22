package mx.j2.recommend.recall.impl;

import com.newrelic.api.agent.Trace;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BadgeDocument;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.MXCollectionUtils;
import mx.j2.recommend.util.MXStringUtils;

import java.util.List;

public class PublisherBadgeInfoCreateTimeRecall extends BaseRecall<BaseDataCollection> {

//        cols.add("max_days");
//        cols.add("max_weeks");
//        cols.add("total_days");

    @Override
    @Trace(dispatcher = true)
    public void recall(BaseDataCollection dc) {
        long ret = MXDataSource.badgeCA().getTime(dc.req.resourceId, dc.req.resourceType, dc.req.tabId, dc.req.num);

        BadgeDocument doc = new BadgeDocument();
        doc.setTimestamp(ret);
        dc.mergedList.add(doc);
    }

    @Override
    public boolean skip(BaseDataCollection baseDC) {
        return MXStringUtils.isEmpty(baseDC.req.resourceId);
    }
}