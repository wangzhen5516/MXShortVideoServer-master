package mx.j2.recommend.filter.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * 过滤规则：
 * app_name 是 instagram 且宽高比大于 0.9
 */
public class InstagramAppWHRatioFilter extends BaseFilter {

    private static final String APP_NAME = "instagram";

    @Override
    public boolean isFilted(BaseDocument doc, BaseDataCollection baseDc) {
        if (null == doc) {
            return true;
        }

        // 无效的尺寸，不过滤，放过
        if (doc.thumbnailWidth == 0 || doc.thumbnailHeight == 0) {
            return false;
        }

        // 如果不是 instagram app 的视频，不过滤
        if (!APP_NAME.equals(doc.appName)) {
            return false;
        }

        // 宽屏视频，过滤
        float whRatio = (float) doc.thumbnailWidth / doc.thumbnailHeight;
        if (whRatio >= 0.75f) {
            return true;
        }

        return false;
    }
}
