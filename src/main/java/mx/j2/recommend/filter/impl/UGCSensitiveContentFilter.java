package mx.j2.recommend.filter.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.util.MXStringUtils;

/**
 * 新版的UGC敏感内容过滤器
 * 如果人工打分存在，并且人工打分 <= 0，过滤;
 * 字段	取值	非个人页	个人页
 * is_terrorism null 0	1
 * is_terrorism	0	0	1
 * is_terrorism	1	1	1
 * is_porn 	null	0	1
 * is_porn 	0	0	1
 * is_porn 	1	1	1
 * is_reported	null	1	1
 * is_reported	0	1	1
 * is_reported	1	0	1
 * appeal_status	null	1	1
 * appeal_status	0	0	1
 * appeal_status	1	1	1
 * appeal_status	2	0	0
 * is_disabled	null	1	1
 * is_disabled	0	1	1
 * is_disabled	1	0	0
 */
public class UGCSensitiveContentFilter extends BaseFilter {

    @Override
    public boolean isFilted(BaseDocument doc, BaseDataCollection baseDc) {
        // 非 UGC，不过滤
        if (!doc.isUgc()) {
            return false;
        }
        //如果是看自己主页
        if (baseDc != null && baseDc.req != null && baseDc.req.userInfo != null &&
                MXStringUtils.isNotEmpty(baseDc.req.resourceId) && MXStringUtils.isNotEmpty(baseDc.req.userInfo.userId) &&
                baseDc.req.resourceId.equals(baseDc.req.userInfo.userId)) {
            //低版本等同非个人主页过滤
            if (MXStringUtils.isEmpty(baseDc.req.clientVersion) || baseDc.req.clientVersion.compareTo("10032") <= 0) {
                return checkinGeneralPage(doc);
            } else {
                return checkinMyPage(doc);
            }
        } else {//非个人主页过滤
            return checkinGeneralPage(doc);
        }
    }

    private boolean checkinMyPage(BaseDocument doc) {
        //apeal_status
        if (doc.exists(BaseDocument.FIELD_EXISTS_FLAG_APPEAL_STATUS) && (doc.getAppealStatus() == 2)) {
            return true;
        }

        //is_disabled
        return doc.exists(BaseDocument.FIELD_EXISTS_FLAG_IS_DISABLED) && doc.isDisabled();
    }

    private boolean checkinGeneralPage(BaseDocument doc) {
        //人工打分
        if (doc.exists(BaseDocument.FIELD_EXISTS_FLAG_HUMAN_SCORE)) {
            if (doc.getHumanScore() <= 0) {
                return true;
            }
        } else {
            if (!doc.exists(BaseDocument.FIELD_EXISTS_FLAG_IS_TERRORISM) && !doc.exists(BaseDocument.FIELD_EXISTS_FLAG_IS_PORN)) {
                return true;
            }

            //is_terrorism
            if (doc.getIsTerrorism() == 1) {
                return true;
            }

            //is_porn
            if (doc.getIsPorn() == 1) {
                return true;
            }
        }

        //reported
        if (doc.exists(BaseDocument.FIELD_EXISTS_FLAG_IS_REPORTED) && doc.isReported()) {
            return true;
        }

        //apeal_status
        if (doc.exists(BaseDocument.FIELD_EXISTS_FLAG_APPEAL_STATUS) && (doc.getAppealStatus() == 0 || doc.getAppealStatus() == 2)) {
            return true;
        }

        //is_disabled
        return doc.exists(BaseDocument.FIELD_EXISTS_FLAG_IS_DISABLED) && doc.isDisabled();
    }
}
