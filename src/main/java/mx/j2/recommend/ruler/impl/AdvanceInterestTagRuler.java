package mx.j2.recommend.ruler.impl;

import mx.j2.recommend.data_model.data_collection.FeedDataCollection;
import mx.j2.recommend.thrift.Result;
import mx.j2.recommend.util.MXJudgeUtils;

import java.util.ArrayList;
import java.util.List;

public class AdvanceInterestTagRuler extends BaseRuler<FeedDataCollection> {
    private static final String RECALL_NAME = "InterestTagRecall";

    @Override
    public boolean skip(FeedDataCollection data) {
        return MXJudgeUtils.isEmpty(data.req.interestTagList);
    }

    @Override
    public void rule(FeedDataCollection dc) {
        List<Result> interestTagResultList = new ArrayList<>();
        dc.data.result.resultList.forEach(result -> {
                    if (RECALL_NAME.equals(result.internalUse.recallName)) {
                        interestTagResultList.add(result);
                    }
                }
        );
        if (MXJudgeUtils.isEmpty(interestTagResultList)) {
            return;
        }
        dc.data.result.resultList.removeAll(interestTagResultList);
        dc.data.result.resultList.addAll(0, interestTagResultList);

    }
}
