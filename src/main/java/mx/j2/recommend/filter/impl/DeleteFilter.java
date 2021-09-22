package mx.j2.recommend.filter.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;

/**
 * @author liangjie.feng
 * @version 1.0
 * @date 2021/5/27 下午2:28
 * @description
 */
public class DeleteFilter extends BaseFilter {

    @Override
    public boolean isFilted(BaseDocument doc, BaseDataCollection dc) {
        if (null == doc) {
            return true;
        }

        return doc.isDelete;
    }
}
