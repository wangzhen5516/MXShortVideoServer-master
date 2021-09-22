package mx.j2.recommend.filter.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.MXJudgeUtils;
import mx.j2.recommend.util.MXStringUtils;

/**
 * @Author: Qi Mao
 * @Date: 12/3/2020
 * description: 从ca里获取用户长期不喜欢的publisher名单，如果视频是这些作者发布的，则过滤掉
 */

public class LongTermUserDislikePubVideoFilter extends BaseFilter{
    private static final int PUB_LENGTH = 100;

    /**
     *
     * @param doc
     * @param dc
     * @return
     * description: 判断视频是否被过滤
     */
    @Override
    public boolean isFilted(BaseDocument doc, BaseDataCollection dc) {
        if(MXJudgeUtils.isNotEmpty(dc.longTermuserDislikePubIds)){
            if(dc.longTermuserDislikePubIds.contains(doc.publisher_id)){
                return true;
            }
        }
        return false;
    }


    /**
     *
     * @param dc
     * description: 获取用户长期不喜欢的publisher名单
     */
    @Override
    public boolean prepare(BaseDataCollection dc) {
        if(MXJudgeUtils.isNotEmpty(dc.longTermuserDislikePubIds)){
            dc.longTermuserDislikePubIds.clear();
        }
        String uuId = dc.client.user.uuId;
        if(MXStringUtils.isNotEmpty(uuId)){
            MXDataSource.strategyCA().setUserLongTermDislikePublisherList(dc,PUB_LENGTH);
        }

        return true;
    }
}
