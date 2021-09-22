package mx.j2.recommend.ruler.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.thrift.Result;
import mx.j2.recommend.util.MXJudgeUtils;

import java.util.ArrayList;
import java.util.List;

public class FirstRequestSortRuler extends BaseRuler<BaseDataCollection> {

    @Override
    public boolean skip(BaseDataCollection dc) {
        if (MXJudgeUtils.isNotEmpty(dc.req.nextToken) || MXJudgeUtils.isEmpty(dc.data.result.resultList)) {
            return true;
        }
        return false;
    }

    @Override
    public void rule(BaseDataCollection dc) {
        List<Result> res = new ArrayList<>(dc.data.result.resultList);
        res.sort(((o1, o2) -> Double.compare(o2.internalUse.heatScore2, o1.internalUse.heatScore2)));

        dc.data.result.resultList.clear();
        dc.data.result.resultList.addAll(res);
    }
}
