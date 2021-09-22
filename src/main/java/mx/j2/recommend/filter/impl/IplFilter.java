package mx.j2.recommend.filter.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;

/**
 * @author liangjie.feng
 * @version 1.0
 * @date 2021/4/1 下午8:15
 * @description
 */
public class IplFilter extends BaseFilter {
    @Override
    public boolean isFilted(BaseDocument doc, BaseDataCollection dc) {
        if (null == doc) {
            return true;
        }

        if (doc.isIpl && doc.humanReviewStatus != 1) {
            return true;
        }

        return false;
    }
}
