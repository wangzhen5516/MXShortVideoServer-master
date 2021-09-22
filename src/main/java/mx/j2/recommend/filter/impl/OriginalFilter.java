package mx.j2.recommend.filter.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;

/**
 * @ Author     ：xinag.zhou
 * @ Date       ：Created in 下午4:05 2021/1/15
 * @ Description：是否原创
 */

public class OriginalFilter extends BaseFilter {
    @Override
    public boolean isFilted(BaseDocument doc, BaseDataCollection dc) {
        return doc.isNotOriginal;
    }
}
