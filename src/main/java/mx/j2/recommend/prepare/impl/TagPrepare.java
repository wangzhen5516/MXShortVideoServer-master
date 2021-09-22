package mx.j2.recommend.prepare.impl;

import mx.j2.recommend.data_model.UserProfile;
import mx.j2.recommend.data_model.UserProfileTag;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;

import java.util.Set;

/**
 * @author zhendong.wang
 * @version 1.0
 * @date 2021/7/9 下午2:26
 * @description 标签准备
 */
@SuppressWarnings("unused")
public class TagPrepare extends BaseUserProfilePrepare<UserProfileTag,
        Set<UserProfile.Tag>, BaseDataCollection> {

    UserProfileTag newData() {
        return new UserProfileTag();
    }
}
