package mx.j2.recommend.filter.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;

public class BigVFilter extends BaseFilter{
    @Override
    public boolean isFilted(BaseDocument doc, BaseDataCollection dc) {
        if(doc.isBigV()){
            return false;
        }
        return true;
    }
}
