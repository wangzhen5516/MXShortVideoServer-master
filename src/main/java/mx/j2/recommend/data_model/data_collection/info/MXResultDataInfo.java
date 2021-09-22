package mx.j2.recommend.data_model.data_collection.info;

import mx.j2.recommend.thrift.Result;

import java.util.ArrayList;
import java.util.List;

/**
 * @author zhendong.wang
 * @version 1.0
 * @date 2021/6/26 下午12:36
 * @description 结果数据信息
 */
public class MXResultDataInfo extends MXBaseDCInfo {
    /**
     * 结果列表。
     */
    public List<Result> resultList;

    /**
     * 用于记录result数量（经过所有process后的数量）
     */
    public int resultListSize;

    MXResultDataInfo() {
        resultList = new ArrayList<>();
        resultListSize = 0;
    }

    @Override
    public void clean() {
        resultList = null;
        resultList = new ArrayList<>();
        resultListSize = 0;
    }
}
