package mx.j2.recommend.filter.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;

/**
 * @ Author     ：zhongrenli
 * @ Date       ：Created in 下午6:53 2018/12/5
 * @ Description：${description}
 */
public class DecryptNotMarkContentFilter extends BaseFilter {

    @Override
    public boolean isFilted(BaseDocument doc, BaseDataCollection baseDc) {
        if (null == doc) {
            return true;
        }

        if (doc.decryptSign == 1 && doc.specialSign == 0) {
            return true;
        }

        return false;
    }

}
