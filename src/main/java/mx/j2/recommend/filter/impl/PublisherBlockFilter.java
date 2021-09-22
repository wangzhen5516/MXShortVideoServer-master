package mx.j2.recommend.filter.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;

/**
 * 黑名单过滤
 */
@SuppressWarnings("unused")
public class PublisherBlockFilter extends BaseFilter {

    @Override
    public boolean isFilted(BaseDocument doc, BaseDataCollection baseDc) {
        if (baseDc.req.blockPublisherList != null
                && baseDc.req.blockPublisherList.contains(doc.publisher_id)) {
            return true;
        }

        return false;
    }
}
