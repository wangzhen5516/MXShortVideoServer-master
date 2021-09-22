package mx.j2.recommend.mixer.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.util.MXJudgeUtils;

/**
 * @author zhendong.wang
 * @version 1.0
 * @date 2021/2/26 下午4:42
 * @description B 组实验：如果 11 级池子耗尽，此路召回(UserProfileTagRedisRecall{user_profile_tag_redis})就多混
 */
public class MultiListMixerTestB extends MultiListMixer {

    @Override
    public float getCount(BaseDataCollection dc) {
        if (MXJudgeUtils.isEmpty(dc.poolToDocumentListMap.get("taka_flowpool_lv11_v2"))) {
            return 0.13F;
        }
        return super.getCount(dc);
    }
}
