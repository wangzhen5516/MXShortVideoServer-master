package mx.j2.recommend.filter.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.util.MXStringUtils;

/**
 * @author qiqi
 * @date 2020-12-12 17:02
 */
public class NoDescriptionBigVFilter extends BaseFilter {

    @Override
    public boolean isFilted(BaseDocument doc, BaseDataCollection dc) {
        if (doc == null) {
            return true;
        }
        /*来自大V且没描述*/
        return MXStringUtils.isBlank(doc.description) && doc.isBigV;
    }
}
