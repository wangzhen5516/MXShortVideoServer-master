package mx.j2.recommend.component.list.skip.impl;

import mx.j2.recommend.component.list.skip.base.BaseDCSkip;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_source.UserProfileDataSource;

/**
 * 非新用户跳过
 */
@SuppressWarnings("unused")
public final class NonNewUserSkip extends BaseDCSkip {

    @Override
    public boolean skip(BaseDataCollection data) {
        return !UserProfileDataSource.isPureNewUser(data);
    }
}
