package mx.j2.recommend.filter.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;

/**
 * @author qiqi
 * @date 2021-01-04 11:58
 */
public class UgcWaterMarkFilter extends BaseFilter {

    private static int IS_DELOGO = 2;

    @Override
    public boolean isFilted(BaseDocument doc, BaseDataCollection dc) {
        if (doc == null) {
            return true;
        }
        /*是ugc视频且有水印需要过滤*/
        return doc.isUgc() && doc.waterMark == IS_DELOGO;
    }
}
