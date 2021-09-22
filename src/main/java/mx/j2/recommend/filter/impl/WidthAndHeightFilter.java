package mx.j2.recommend.filter.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.util.DefineTool;

/**
 * @author zhongrenli
 * @ Author     ：zhongrenli
 * @ Date       ：Created in 下午6:53 2018/12/5
 * @ Description：${description}
 */
public class WidthAndHeightFilter extends BaseFilter {
    private static final double LENGTH_WIDTH_RATIO = 2.5;
    private static final double LENGTH_WIDTH_LINE = 0.9;

    private static final String NO_NEED_FILTER_APP_NAME = "tiktok";

    @Override
    public boolean isFilted(BaseDocument doc, BaseDataCollection baseDc) {
        if (null == doc) {
            return true;
        }

        if (doc.isNoNeedWidthAndHeightFilt) {
            return false;
        }

        if (NO_NEED_FILTER_APP_NAME.equals(doc.appName)) {
            return false;
        }


        if (DefineTool.TabInfoEnum.HOT.getId().equals(baseDc.req.getTabId())) {
            if (doc.thumbnailWidth == 0 || doc.thumbnailHeight == 0) {
                return true;
            }

            if ((double) doc.thumbnailHeight / (double) doc.thumbnailWidth < LENGTH_WIDTH_LINE) {
                return true;
            }

            // 高图比例阈值 > 2.5 的过滤
            if ((double) doc.thumbnailHeight / (double) doc.thumbnailWidth > LENGTH_WIDTH_RATIO) {
                return true;
            }
        }

        if (DefineTool.TabInfoEnum.STATUS.getId().equals(baseDc.req.getTabId())) {
            if (doc.thumbnailWidth == 0 || doc.thumbnailHeight == 0) {
                return true;
            }

            if ((double) doc.thumbnailHeight / (double) doc.thumbnailWidth >= LENGTH_WIDTH_LINE) {
                return true;
            }

            // 宽图比例阈值 > 2.5 的过滤
            if ((double) doc.thumbnailWidth / (double) doc.thumbnailHeight > LENGTH_WIDTH_RATIO) {
                return true;
            }
        }

        return false;
    }

}
