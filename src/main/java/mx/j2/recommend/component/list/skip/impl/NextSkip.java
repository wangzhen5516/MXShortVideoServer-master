package mx.j2.recommend.component.list.skip.impl;

import mx.j2.recommend.component.list.skip.base.BaseDCSkip;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.util.MXJudgeUtils;

/**
 * 翻页了（不是第一页）要跳过
 */
@SuppressWarnings("unused")
public final class NextSkip extends BaseDCSkip {

    @Override
    public boolean skip(BaseDataCollection data) {
        return MXJudgeUtils.isNotEmpty(data.req.nextToken);
    }
}
