package mx.j2.recommend.component.list.skip.impl;

import mx.j2.recommend.component.list.skip.base.BaseDCSkip;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;

/**
 * 不跳过，必须配置 skip 的情况下可以配这个，作为默认配置
 */
@SuppressWarnings("unused")
public final class NoSkip extends BaseDCSkip {

    @Override
    public boolean skip(BaseDataCollection data) {
        return false;
    }
}
