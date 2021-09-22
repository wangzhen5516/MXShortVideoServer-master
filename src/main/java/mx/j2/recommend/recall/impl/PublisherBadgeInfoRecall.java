package mx.j2.recommend.recall.impl;

import com.newrelic.api.agent.Trace;
import mx.j2.recommend.component.configurable.config.RecallConfig;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BadgeDocument;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_model.document.ShortDocument;
import mx.j2.recommend.hystrix.redis.ZrevRangePvCommand;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXCollectionUtils;
import mx.j2.recommend.util.MXJudgeUtils;
import mx.j2.recommend.util.MXStringUtils;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class PublisherBadgeInfoRecall extends BaseRecall<BaseDataCollection> {

//        cols.add("max_days");
//        cols.add("max_weeks");
//        cols.add("total_days");

    @Override
    @Trace(dispatcher = true)
    public void recall(BaseDataCollection dc) {
        List<Object> rets = MXDataSource.badgeCA().getBadgeCount(dc.req.resourceId);

        if(MXCollectionUtils.isNotEmpty(rets) && rets.size() == 3) {
            BadgeDocument doc = new BadgeDocument();
            doc.setMaxDays(Integer.parseInt(rets.get(0).toString()));
            doc.setMaxWeeks(Integer.parseInt(rets.get(1).toString()));
            doc.setTotalDays(Integer.parseInt(rets.get(2).toString()));

            dc.mergedList.add(doc);
        }
    }

    @Override
    public boolean skip(BaseDataCollection baseDC) {
        return MXStringUtils.isEmpty(baseDC.req.resourceId);
    }
}