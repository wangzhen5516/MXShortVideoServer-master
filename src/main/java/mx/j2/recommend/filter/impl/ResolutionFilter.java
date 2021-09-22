package mx.j2.recommend.filter.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;

public class ResolutionFilter extends BaseFilter {
    private final double RESOLUTION_RATIO = 16.0d/9.0d;

    @Override
    public boolean isFilted(BaseDocument doc, BaseDataCollection dc) {
        if (doc.qqMetaWidth < 540 || doc.qqMetaHeight == 0) {
            return true;
        }

        return doc.qqMetaHeight < (doc.qqMetaWidth * RESOLUTION_RATIO);
    }
}
