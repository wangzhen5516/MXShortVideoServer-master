package mx.j2.recommend.ruler.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.thrift.Result;

import java.util.ArrayList;
import java.util.List;

public class FollowingBigVRuler extends BaseRuler<BaseDataCollection> {

    @Override
    public boolean skip(BaseDataCollection data) {
        return false;
    }

    @Override
    public void rule(BaseDataCollection dc) {
        List<Result> bigVList = new ArrayList<>(dc.data.result.resultList.size());
        List<Result> notBigVList = new ArrayList<>(dc.data.result.resultList.size());
        List<Result> tempList = new ArrayList<>(dc.data.result.resultList.size());
        for (Result result : dc.data.result.resultList) {
            if (dc.userUploadVideoIDList.contains(result.shortVideo.id) && dc.bigVPublishVideoIdList.contains(result.shortVideo.id)) {
                bigVList.add(result);
            } else {
                notBigVList.add(result);
            }
        }
        tempList.addAll(bigVList);
        tempList.addAll(notBigVList);
        dc.data.result.resultList = tempList;
    }
}
