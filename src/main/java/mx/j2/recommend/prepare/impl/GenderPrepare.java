package mx.j2.recommend.prepare.impl;

import mx.j2.recommend.data_model.UserProfile;
import mx.j2.recommend.data_model.UserProfileGender;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;

/**
 * @author zhendong.wang
 * @version 1.0
 * @date 2021/7/9 下午2:27
 * @description 性别准备
 */
@SuppressWarnings("unused")
public class GenderPrepare extends BaseUserProfilePrepare<UserProfileGender,
        UserProfile.Gender, BaseDataCollection> {

    @Override
    UserProfileGender newData() {
        return new UserProfileGender();
    }
}
