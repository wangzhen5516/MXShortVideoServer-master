package mx.j2.recommend.component.list.skip.impl;

import mx.j2.recommend.component.list.skip.base.BaseDCSkip;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;

/**
 * 总是跳过，移除组件功能的情况下可以配这个
 */
@SuppressWarnings("unused")
public final class AlwaysSkip extends BaseDCSkip {

    @Override
    public boolean skip(BaseDataCollection data) {
        return true;
    }
}
