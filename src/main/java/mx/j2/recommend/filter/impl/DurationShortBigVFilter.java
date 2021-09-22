package mx.j2.recommend.filter.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;

/**
 * @author qiqi
 * @date 2020-12-12 17:03
 */
public class DurationShortBigVFilter extends BaseFilter {

    private static final long DURATION_LIMIT = 7000;

    @Override

    public boolean isFilted(BaseDocument doc, BaseDataCollection dc) {
        if (doc == null) {
            return true;
        }
        /*外部duration可能没数据*/
        long baseDuration = doc.duration == 0 ? doc.innerDuration : doc.duration;
        /*来自大V且时长小于7s*/
        return doc.isBigV && baseDuration < DURATION_LIMIT;
    }
}
