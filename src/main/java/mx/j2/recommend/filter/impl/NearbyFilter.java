package mx.j2.recommend.filter.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.util.MXStringUtils;

public class NearbyFilter extends BaseFilter {
    /**
     * 附近的人接口， 自己看不到自己的视频
     *
     * @param doc
     * @param baseDc
     * @return
     */
    @Override
    public boolean isFilted(BaseDocument doc, BaseDataCollection baseDc) {
        if (doc == null) {
            return true;
        }

        //如果这个视频是自己的，过滤掉
        if (baseDc != null && baseDc.req != null && baseDc.req.userInfo != null &&
                MXStringUtils.isNotEmpty(doc.publisher_id) && MXStringUtils.isNotEmpty(baseDc.req.userInfo.userId) &&
                doc.publisher_id.equals(baseDc.req.userInfo.userId)) {
            return true;
        }
        return false;
    }
}
