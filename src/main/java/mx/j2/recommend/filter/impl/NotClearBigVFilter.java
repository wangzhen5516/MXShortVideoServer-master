package mx.j2.recommend.filter.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;

/**
 * @author qiqi
 * @date 2020-12-12 17:01
 */
public class NotClearBigVFilter extends BaseFilter {

    @Override
    public boolean isFilted(BaseDocument doc, BaseDataCollection dc) {
        if (doc == null) {
            return true;
        }
        /*属于大V且分数大于20（不清晰）*/
        return doc.isBigV && doc.niqeScore >= 20;
    }
}
