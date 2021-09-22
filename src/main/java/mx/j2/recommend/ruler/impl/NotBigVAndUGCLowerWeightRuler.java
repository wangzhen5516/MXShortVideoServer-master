package mx.j2.recommend.ruler.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.thrift.Result;
import mx.j2.recommend.util.MXJudgeUtils;

import java.util.ArrayList;
import java.util.List;

public class NotBigVAndUGCLowerWeightRuler extends BaseRuler<BaseDataCollection> {

    @Override
    public boolean skip(BaseDataCollection dc) {
        if (MXJudgeUtils.isNotEmpty(dc.req.nextToken) || MXJudgeUtils.isEmpty(dc.data.result.resultList)) {
            return true;
        }
        return false;
    }

    @Override
    public void rule(BaseDataCollection dc) {
        List<Result> topRes = new ArrayList<>();
        List<Result> result = new ArrayList<>(dc.data.result.resultList);
        for (Result r : result) {
            if (r.internalUse.isBigV || !r.internalUse.isUgc) {
                topRes.add(r);
            }
            if (topRes.size() >= 5) {
                break;
            }
        }
        result.removeAll(topRes);
        topRes.addAll(result);
        dc.data.result.resultList.clear();
        dc.data.result.resultList.addAll(topRes);
    }
}
