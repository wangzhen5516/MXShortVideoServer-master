package mx.j2.recommend.packer.impl;

import com.newrelic.api.agent.Trace;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.thrift.PublisherInfo;
import mx.j2.recommend.thrift.Result;
import mx.j2.recommend.thrift.ShortVideo;
import mx.j2.recommend.util.MXJudgeUtils;

/**
 * @author DuoZhao
 * @Date 2021/03/18
 * @Description Follow Card Packer
 */
public class Mx_Recommend_Follow_Card_Packer extends BasePacker {
    // PM要求的card最小数量，小于则返回空
    private final int MIN_LIMITATION = 4;

    @Override
    @Trace(dispatcher = true)
    public void pack(BaseDataCollection baseDc) {
        if (MXJudgeUtils.isEmpty(baseDc.followCardKOLIds) || baseDc.followCardKOLIds.size() < MIN_LIMITATION) {
            return;
        }

        for (PublisherInfo info: baseDc.followCardKOLIds) {
            Result result = new Result();
            result.setPublisherInfo(info);
            result.setShortVideo(new ShortVideo());
            baseDc.data.result.resultList.add(result);
        }
    }
}
