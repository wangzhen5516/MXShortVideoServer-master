package mx.j2.recommend.component.list.check.impl;

import mx.j2.recommend.component.list.check.base.BaseCheck;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.util.DefineTool;

/**
 * 该项的过滤检测，返回 true 即需要过滤掉
 */
@SuppressWarnings("unused")
public final class ClientVersionFilterCheck extends BaseCheck<BaseDocument, BaseDataCollection> {

    @Override
    public boolean check(BaseDocument doc, BaseDataCollection dc) {
        if (isAndroidClient(dc.req.platformId)) {
            return filterAndroid(dc.req.clientVersion, doc.clientVersionInfo.getAndroid());
        } else if (isIosClient(dc.req.platformId)) {
            return filterIos("", doc.clientVersionInfo.getIos());
        }

        return false;
    }

    private boolean isAndroidClient(String platformId) {
        return DefineTool.PlatformsEnum.ANDROID.getIndex().equals(platformId);
    }

    private boolean isIosClient(String platformId) {
        return DefineTool.PlatformsEnum.IOS.getIndex().equals(platformId);
    }

    private boolean filterAndroid(String reqVersion, int minVersion) {
        // 解析请求中的版本号
        int reqClientVersion = -1;
        try {
            reqClientVersion = Integer.parseInt(reqVersion);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // 版本号解析错误，不过滤
        if (reqClientVersion == -1) {
            return false;
        }

        if (minVersion != -1) {
            return reqClientVersion < minVersion;
        }

        return false;
    }

    /**
     * IOS 未上线，暂不过滤
     */
    private boolean filterIos(String reqVersion, String minVersion) {
        return false;
    }
}
