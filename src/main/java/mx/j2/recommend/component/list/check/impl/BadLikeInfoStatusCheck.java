package mx.j2.recommend.component.list.check.impl;

import mx.j2.recommend.component.list.check.base.BaseCheck;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;

@SuppressWarnings("unused")
public final class BadLikeInfoStatusCheck extends BaseCheck<BaseDocument, BaseDataCollection> {

    @Override
    public boolean check(BaseDocument doc, BaseDataCollection dc) {
        return doc.likeInfoStatus == -1;
    }
}
