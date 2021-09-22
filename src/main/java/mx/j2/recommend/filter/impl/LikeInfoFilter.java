package mx.j2.recommend.filter.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;

/**
 * @author qiqi
 * @date 2021-02-20 16:34
 *
 * Use class DefaultFilter instead
 * @see StandardFilter
 */
@Deprecated
public class LikeInfoFilter extends BaseFilter {

    @Override
    public boolean isFilted(BaseDocument doc, BaseDataCollection dc) {
        if (doc == null) {
            return true;
        }
        return doc.likeInfoStatus == -1;
    }
}
