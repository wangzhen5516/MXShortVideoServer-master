package mx.j2.recommend.ruler.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.thrift.Result;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author: xiaoling.zhu
 * @Date: 2021-02-03
 */

public class LikeCountDiverseRuler extends BaseRuler<BaseDataCollection> {

    @Override
    public boolean skip(BaseDataCollection data) {
        return false;
    }

    @Override
    public void rule(BaseDataCollection dc) {
        List<Result> resultList = new ArrayList<>();
        List<Result> tempList = new ArrayList<>();
        for (Result result : dc.data.result.resultList){
            if(result.shortVideo.likeCount > 500){
                if(tempList.size()>0){
                    resultList.add(tempList.remove(0));
                }
                resultList.add(result);
            }else {
                tempList.add(result);
            }
        }
        if(tempList.size()>0){
            resultList.addAll(tempList);
        }
        dc.data.result.resultList.clear();
        dc.data.result.resultList.addAll(resultList);
    }
}
