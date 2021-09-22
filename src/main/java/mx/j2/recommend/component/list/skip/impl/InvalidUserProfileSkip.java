package mx.j2.recommend.component.list.skip.impl;

import mx.j2.recommend.component.list.skip.base.BaseDCSkip;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.util.MXStringUtils;

/**
 * 非法的个性化用户跳过
 */
@SuppressWarnings("unused")
public final class InvalidUserProfileSkip extends BaseDCSkip {

    @Override
    public boolean skip(BaseDataCollection dc) {
        if (dc.req == null || dc.req.userInfo == null) {
            return true;
        }

        return MXStringUtils.isBlank(dc.req.userInfo.getUuid());
    }
}
