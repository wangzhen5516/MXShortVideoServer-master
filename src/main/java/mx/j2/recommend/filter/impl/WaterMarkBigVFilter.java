package mx.j2.recommend.filter.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;

/**
 * @author qiqi
 * @date 2020-12-12 16:57
 */
public class WaterMarkBigVFilter extends BaseFilter {
    private static final int IS_DELOGO = 2;

    @Override
    public boolean isFilted(BaseDocument doc, BaseDataCollection dc) {
        if (doc == null) {
            return true;
        }
        /*该视频来源于大V且有水印*/
        return doc.isBigV && doc.waterMark == IS_DELOGO && "PoolRecall".equals(doc.recallName) && doc.getPoolPriority()<7 ;
    }
}
