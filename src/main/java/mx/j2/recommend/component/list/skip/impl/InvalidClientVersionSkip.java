package mx.j2.recommend.component.list.skip.impl;

import mx.j2.recommend.component.list.skip.base.BaseDCSkip;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.util.MXStringUtils;

@SuppressWarnings("unused")
public final class InvalidClientVersionSkip extends BaseDCSkip {

    @Override
    public boolean skip(BaseDataCollection data) {
        return MXStringUtils.isEmpty(data.req.clientVersion);
    }
}
