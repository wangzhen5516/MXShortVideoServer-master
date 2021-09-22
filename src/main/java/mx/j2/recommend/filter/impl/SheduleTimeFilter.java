package mx.j2.recommend.filter.impl;

import mx.j2.recommend.data_model.data_collection.BannerDataCollection;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BannerDocument;
import mx.j2.recommend.data_model.document.BaseDocument;

/**
 * 针对banner的start_time end_time过滤
 *
 * @author xiang.zhou
 * @date 2020-07-14 11:16
 */
public class SheduleTimeFilter extends BaseFilter {


    @Override
    public boolean isFilted(BaseDocument doc, BaseDataCollection baseDc) {
        if (doc == null) {
            return true;
        }

        if (!(baseDc instanceof BannerDataCollection)) {
            return false;
        }

        long currentTime = System.currentTimeMillis();
        BannerDocument document = (BannerDocument) doc;

        if (document.getStartTime() > currentTime || document.getEndTime() < currentTime) {
            return true;
        }
        return false;
    }
}
