package mx.j2.recommend.component.list.skip.impl;

import mx.j2.recommend.component.list.skip.base.BaseDCSkip;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.util.MXJudgeUtils;

@SuppressWarnings("unused")
public final class InvalidResourceIdSkip extends BaseDCSkip {

    @Override
    public boolean skip(BaseDataCollection data) {
        return MXJudgeUtils.isEmpty(data.req.resourceId);
    }
}
