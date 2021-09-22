package mx.j2.recommend.filter.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_source.LocalCacheDataSource;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.IsFriendUtil;

/**
 * @author DuoZhao
 * public, friend, private过滤
 */

public class ViewPrivacyFilter extends BaseFilter {
    @Override
    public boolean isFilted(BaseDocument doc, BaseDataCollection dc) {
        if (doc.viewPrivacy == 2) {
            return true;
        }
        if (doc.getPublisher_id().equals(dc.req.userInfo.userId)) {
            return false;
        }
        if (dc.req.getUserInfo().getUserId().equals(dc.req.getUserInfo().getUuid())) {
            return doc.getViewPrivacy() != 0;
        }
        if (doc.getViewPrivacy() == 1) { // Friends
            LocalCacheDataSource localCacheDataSource = MXDataSource.cache();
            String localCacheKey = String.format("%s_%s_%s", this.getName(), dc.req.userInfo.userId, doc.getPublisher_id());
            Integer status = localCacheDataSource.getIsFriendCache(localCacheKey);

            if (null == status) {
                status = IsFriendUtil.isFriend(dc.req.userInfo.userId, doc.getPublisher_id());
                // 防止熔断返回null
                if (null == status) {
                    return false;
                }
                localCacheDataSource.setIsFriendCache(localCacheKey, status);
            }
            // status 1: friend; status 0: not friend
            return status == 0;
        }
        // 等于2为Private，返回true被删掉；等于0为Public，返回false不过滤
        return doc.getViewPrivacy() == 2;
    }
}
