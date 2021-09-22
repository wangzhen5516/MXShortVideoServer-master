package mx.j2.recommend.filter.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.util.MXStringUtils;

public class ReportFilter extends BaseFilter {
    @Override
    public boolean isFilted(BaseDocument doc, BaseDataCollection baseDc) {
        if (doc == null) {
            return true;
        }

        //如果该视频被举报，只能自己看到，其他人看不到
        if (doc.isReported) {
            //如果是看自己主页
            if (baseDc != null && baseDc.req != null && baseDc.req.userInfo != null &&
                    MXStringUtils.isNotEmpty(baseDc.req.resourceId) && MXStringUtils.isNotEmpty(baseDc.req.userInfo.userId) &&
                    baseDc.req.resourceId.equals(baseDc.req.userInfo.userId)) {
                return false;
            } else {
                return true;
            }
        }
        return false;
    }
}
