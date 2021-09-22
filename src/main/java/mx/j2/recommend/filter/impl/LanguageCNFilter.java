package mx.j2.recommend.filter.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.util.MXCollectionUtils;

/**
 * @ Author     ：xiang.zhou
 * @ Date       ：Created in 下午6:53 2020/7/5
 */
public class LanguageCNFilter extends BaseFilter {

    @Override
    public boolean isFilted(BaseDocument doc, BaseDataCollection baseDc) {
        if (null == doc) {
            return true;
        }
        if (MXCollectionUtils.isEmpty(doc.languageIdList)) {
            return false;
        }

        return doc.languageIdList.contains("cn");
    }

}
