package mx.j2.recommend.recall.impl;

import com.alibaba.fastjson.JSONObject;
import mx.j2.recommend.data_model.data_collection.FeedDataCollection;
import mx.j2.recommend.data_model.data_collection.info.MXEntityDebugInfo;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXJudgeUtils;

import java.util.Collections;
import java.util.List;
import java.util.Set;

public class InterestTagRecall extends BaseRecall<FeedDataCollection> {
    @Override
    public boolean skip(FeedDataCollection dc) {
        return !dc.req.tabId.equals(DefineTool.TabInfoEnum.HOT.getId());
    }

    @Override
    public JSONObject constructTerms(String matchKey, List<String> idList) {
        JSONObject termsFather = new JSONObject();
        JSONObject terms = new JSONObject();
        terms.put(matchKey, idList);
        termsFather.put("terms", terms);
        return termsFather;
    }

    @Override
    public void recall(FeedDataCollection dc) {
        List<String> interstTagList = MXJudgeUtils.isEmpty(dc.req.interestTagList) ? MXDataSource.profileTagV2().getLanguageAndInterestFromCA(dc.client.user.uuId) : dc.req.interestTagList;
        if (MXJudgeUtils.isEmpty(interstTagList)) {
            return;
        }

        Set<BaseDocument> docList;
        for (String interestTag : interstTagList) {
            docList = MXDataSource.interestTag().getInterestTagDocSet(interestTag);
            if (MXJudgeUtils.isNotEmpty(docList)) {
                docList.forEach(item -> {
                    item.setRecallName(this.getName());

                    // 召回信息
                    MXEntityDebugInfo debugInfo = dc.debug.getDebugInfoByEntityId(item.id);
                    debugInfo.recall.name = this.getName();
                });
                dc.interestTagDocList.addAll(docList);
            }
        }
        if (MXJudgeUtils.isNotEmpty(dc.interestTagDocList)) {
            Collections.shuffle(dc.interestTagDocList);
            dc.syncSearchResultSizeMap.put(this.getName(), dc.interestTagDocList.size());
            dc.resultFromMap.put(this.getName(), DefineTool.RecallFrom.ES.getName());
        }
    }
}
