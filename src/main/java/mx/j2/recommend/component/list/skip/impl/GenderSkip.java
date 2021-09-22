package mx.j2.recommend.component.list.skip.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.component.list.skip.base.BaseDCSkip;
import mx.j2.recommend.data_source.UserProfileDataSource;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.MXStringUtils;

/**
 * 有性别标签的要跳过
 */
@SuppressWarnings("unused")
public final class GenderSkip extends BaseDCSkip {

    @Override
    public boolean skip(BaseDataCollection data) {
        // 从cassandra里得到uuid的userprofile
        UserProfileDataSource userProfileDataSource = MXDataSource.profile();
        String userProfile = userProfileDataSource.getUserProfileByUuId(data.client.user.uuId);
        String gender = userProfileDataSource.getUserGenderInfo(userProfile, data.client.user.userId);

        // 如果有明确的性别配置，则跳过
        //noinspection RedundantIfStatement
        if (MXStringUtils.isNotEmpty(gender) && !gender.equals("unknown")) {
            return true;
        }

        return false;
    }
}
