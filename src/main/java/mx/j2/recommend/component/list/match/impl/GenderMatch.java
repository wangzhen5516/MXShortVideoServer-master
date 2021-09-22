package mx.j2.recommend.component.list.match.impl;

import mx.j2.recommend.component.list.match.base.BaseProfileMatch;
import mx.j2.recommend.data_model.UserProfileGender;
import mx.j2.recommend.data_model.document.BaseDocument;

/**
 * @author zhendong.wang
 * @version 1.0
 * @date 2021/7/12 下午4:36
 * @description 性别匹配
 */
@SuppressWarnings("unused")
public class GenderMatch extends BaseProfileMatch<UserProfileGender> {
    @Override
    public boolean matches(UserProfileGender gender, BaseDocument document) {
        return false;
    }
}
