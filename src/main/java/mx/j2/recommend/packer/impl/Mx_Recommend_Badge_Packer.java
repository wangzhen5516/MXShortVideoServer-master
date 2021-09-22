package mx.j2.recommend.packer.impl;

import com.google.common.collect.Lists;
import com.newrelic.api.agent.Trace;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BadgeDocument;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_model.document.ScoreDocument;
import mx.j2.recommend.data_model.document.ShortDocument;
import mx.j2.recommend.data_model.statistics_document.BaseStatisticsDocument;
import mx.j2.recommend.server.RecommendServer;
import mx.j2.recommend.thrift.*;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXJudgeUtils;
import mx.j2.recommend.util.OptionalUtil;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

import static mx.j2.recommend.util.BaseMagicValueEnum.FEATURE30D;

/**
 * @author xiang.zhou
 * @date 2021/4/19
 */
public class Mx_Recommend_Badge_Packer extends BasePacker {

    @Override
    @Trace(dispatcher = true)
    public void pack(BaseDataCollection dc) {
        for (int i = 0; i < dc.mergedList.size(); i++) {
            BaseDocument doc = dc.mergedList.get(i);
            if (doc == null) {
                continue;
            }
            if (!(doc instanceof BadgeDocument)) {
                continue;
            }
            BadgeDocument bd = (BadgeDocument) doc;
            Badge b = new Badge();
            b.setMaxDays(bd.getMaxDays());
            b.setMaxWeeks(bd.getMaxWeeks());
            b.setTotalDays(bd.getTotalDays());
            b.setTimestamp(bd.getTimestamp());
            dc.data.response.setBadge(b);
        }
    }
}
