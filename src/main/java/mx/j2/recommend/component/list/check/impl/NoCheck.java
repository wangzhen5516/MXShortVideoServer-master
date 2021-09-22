package mx.j2.recommend.component.list.check.impl;

import mx.j2.recommend.component.list.check.base.BaseCheck;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;

/**
 * 不检查，必须配置 check 的情况下可以配这个，作为默认配置
 */
@SuppressWarnings("unused")
public final class NoCheck extends BaseCheck<BaseDocument, BaseDataCollection> {

    @Override
    public boolean check(BaseDocument doc, BaseDataCollection dc) {
        return false;
    }
}
