package mx.j2.recommend.recall.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_source.UserProfileDataSource;

/**
 * @author zhongrenli
 * @ Author     ：zhongrenli
 * @ Date       ：Created in 下午4:05 2018/12/5
 * @ Description：${description}
 */
public class ManualTopRecall1 extends ManualTopRecall {

    private final static String REDIS_KEY = "manual_top_default";

    @Override
    public boolean skip(BaseDataCollection baseDC) {
        return !UserProfileDataSource.isPureNewUser(baseDC);
    }

    @Override
    public String getRedisKey(BaseDataCollection dc) {
        return REDIS_KEY;
    }
}