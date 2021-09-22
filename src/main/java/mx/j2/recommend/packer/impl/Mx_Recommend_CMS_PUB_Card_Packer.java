package mx.j2.recommend.packer.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.thrift.PublisherInfo;
import mx.j2.recommend.thrift.Result;
import mx.j2.recommend.thrift.ShortVideo;
import mx.j2.recommend.util.MXJudgeUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * @Author: xiaoling.zhu
 * @Date: 2021-05-22
 */

public class Mx_Recommend_CMS_PUB_Card_Packer extends BasePacker {
    private static final Logger logger = LogManager.getLogger(Mx_Recommend_CMS_PUB_Card_Packer.class);

    @Override
    public void pack(BaseDataCollection dc) {
        if (MXJudgeUtils.isEmpty(dc.cmsPubCardPubIds)) {
            return;
        }

        for (PublisherInfo info: dc.cmsPubCardPubIds) {
            Result result = new Result();
            result.setPublisherInfo(info);
            result.setShortVideo(new ShortVideo());
            dc.data.result.resultList.add(result);
        }
    }
}
