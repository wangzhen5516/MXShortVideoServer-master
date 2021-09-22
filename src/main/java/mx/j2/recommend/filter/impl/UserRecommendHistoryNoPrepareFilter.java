package mx.j2.recommend.filter.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;

/**
 * @author liangjie.feng
 * @version 1.0
 * @date 2021/5/12 下午7:44
 * @description
 */
public class UserRecommendHistoryNoPrepareFilter extends BaseFilter {

    @Override
    public boolean isFilted(BaseDocument doc, BaseDataCollection baseDc) {
        if (baseDc.historyIdList.contains(doc.id)) {
            return true;
        }
        return false;
    }

}
