package mx.j2.recommend.component.list.match.impl;

import mx.j2.recommend.component.list.match.base.BaseProfileMatch;
import mx.j2.recommend.data_model.UserProfilePublisher;
import mx.j2.recommend.data_model.document.BaseDocument;

/**
 * @author zhendong.wang
 * @version 1.0
 * @date 2021/7/12 下午4:36
 * @description 作者匹配
 */
@SuppressWarnings("unused")
public class PublisherMatch extends BaseProfileMatch<UserProfilePublisher> {
    @Override
    public boolean matches(UserProfilePublisher publisher, BaseDocument document) {
        return false;
    }
}
