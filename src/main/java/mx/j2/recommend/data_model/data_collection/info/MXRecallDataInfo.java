package mx.j2.recommend.data_model.data_collection.info;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.util.MXJudgeUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * @author zhendong.wang
 * @version 1.0
 * @date 2021/6/26 下午12:36
 * @description 召回数据信息
 */
public class MXRecallDataInfo extends MXBaseDCInfo {
    /**
     * 召回结果，所有的召回器数据原则上都可以放这里
     * <p>
     * Key:
     *
     * @see BaseDataCollection.IResult.ListEnum
     * <p>
     * Value:
     * value 可能是 List，也可能是 Map，简单处理，目前不能有别的结构
     * 如果有别的结构且是自定义的结构，就必须给 value 再抽象，以方便过滤操作
     */
    public Map<String, Object> resultMap;

    /**
     * 初始化函数
     */
    MXRecallDataInfo() {
        resultMap = new HashMap<>();
    }

    @Override
    public void clean() {
        resultMap.clear();
    }

    public boolean isEmpty() {
        return MXJudgeUtils.isEmpty(resultMap);
    }
}
