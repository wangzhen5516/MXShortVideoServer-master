package mx.j2.recommend.ruler.impl;

import mx.j2.recommend.thrift.Result;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.util.BaseMagicValueEnum;
import mx.j2.recommend.util.MXJudgeUtils;
import mx.j2.recommend.util.MXStringUtils;


import java.util.ArrayList;
import java.util.List;

/**
 * @author Qi Mao
 * @date 1/28/2021
 * @description 用来给新用户排序，保证新用户的第一把：a.如果高级池子的视频有十个，放到15个的头10个位置；b.如果没有十个，把低级池子的视频放到15个的最末尾
 */
public class NewUserPoolResultRuler extends BaseRuler<BaseDataCollection> {
    private static final int HIGH_LEVEL_VIDEO_NUM = 10;

    @Override
    public boolean skip(BaseDataCollection dc) {
        if (MXStringUtils.isNotEmpty(dc.req.nextToken) || MXJudgeUtils.isEmpty(dc.data.result.resultList) ) {
            return true;
        }
        return false;
    }

    @Override
    public void rule(BaseDataCollection dc) {
        List<Result> highList = new ArrayList<>();
        List<Result> lowList = new ArrayList<>();
        List<Result> resultList = new ArrayList<>(dc.data.result.resultList);
        int counter = 0;
        boolean highEnough = false;
        for (Result res : resultList) {
            String poolLevel = res.internalUse.getPoolLevel();
            if (MXStringUtils.isEmpty(poolLevel)) {
                continue;
            }

            if (poolLevel.equals(BaseMagicValueEnum.HIGH_LEVEL)) {
                highList.add(res);
                counter++;
            } else if (poolLevel.equals(BaseMagicValueEnum.LOW_LEVEL)) {
                lowList.add(res);
            }
            if (counter == HIGH_LEVEL_VIDEO_NUM) {
                highEnough = true;
                break;
            }
        }

        if (highEnough) {
            resultList.removeAll(highList);
            dc.data.result.resultList.clear();
            dc.data.result.resultList.addAll(resultList);
            dc.data.result.resultList.addAll(0,highList);
        } else {
            resultList = new ArrayList<>(dc.data.result.resultList);
            if(MXJudgeUtils.isNotEmpty(lowList)){
                resultList.removeAll(lowList);
                dc.data.result.resultList.clear();
                dc.data.result.resultList.addAll(lowList);
                dc.data.result.resultList.addAll(0,resultList);
            }
        }
    }
}
