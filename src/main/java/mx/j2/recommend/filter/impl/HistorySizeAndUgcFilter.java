package mx.j2.recommend.filter.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;

/**
 * @author qiqi
 * @date 2021-01-05 19:06
 */
public class HistorySizeAndUgcFilter extends BaseFilter {

    private static final int MIN_SIZE = 50;
    private static int size = 0;

    @Override
    public boolean isFilted(BaseDocument doc, BaseDataCollection dc) {
        if (doc == null) {
            return true;
        }
        /*如果当前视频是普通ugc且该用户历史数据不足50过滤*/
        return doc.isUgc && (!doc.isBigV()) && size < MIN_SIZE;
    }

    @Override
    public boolean prepare(BaseDataCollection dc) {
        size = dc.historyIdList.size();
        return true;
    }
}
