package mx.j2.recommend.filter.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXStringUtils;

/**
 * 客户端版本过滤
 * <p>
 * 过滤小于给定版本的视频
 *
 * Use class DefaultFilter instead
 * @see StandardFilter
 */
@Deprecated
@SuppressWarnings("unused")
public class ClientVersionFilter extends BaseFilter {

    @Override
    public boolean isFilted(BaseDocument doc, BaseDataCollection baseDc) {
        // 如果请求中没有版本字段，不过滤
        if (MXStringUtils.isEmpty(baseDc.req.clientVersion)) {
            return false;
        }

        // 如果文档没有版本字段，不过滤
        if (doc.clientVersionInfo == null) {
            return false;
        }

        if (isAndroidClient(baseDc.req.platformId)) {
            return filterAndroid(baseDc.req.clientVersion, doc.clientVersionInfo.getAndroid());
        } else if (isIosClient(baseDc.req.platformId)) {
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
