package mx.j2.recommend.util;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;

/**
 * @author xiang.zhou
 * @description
 * @date 2020-07-08
 */
public class BloomUtil {

    public static String getBloomKey(String uid) {
        if(null == uid) {
            return null;
        } else {
            return uid + "_bloom";
        }
    }

    public static String getGuavaKey(String uid) {
        if(null == uid) {
            return null;
        } else {
            return uid + "_guava";
        }
    }

    public static String getUserIdBloomKey(BaseDataCollection dc) {
        return getBloomKey(getUserId(dc));
    }

    public static String getUserIdGuavaKey(BaseDataCollection dc) {
        return getGuavaKey(getUserId(dc));
    }

    public static String getUuIdBloomKey(BaseDataCollection dc) {
        return getBloomKey(getUuid(dc));
    }

    public static String getUuid(BaseDataCollection baseDc) {
        return baseDc.req.getUserInfo().getUuid();
    }

    // 如果请求中没填userId, 就用uuid来填上, 存bloom历史信息的时候, 统一走userId来存.
    public static String getUserId(BaseDataCollection baseDc) {
        if (baseDc.req.getUserInfo().isSetUserId() && (MXJudgeUtils.isNotEmpty(baseDc.req.getUserInfo().getUserId()))) {
            return baseDc.req.getUserInfo().getUserId();
        } else {
            return baseDc.req.getUserInfo().getUuid();
        }
    }

    public static String getUserFetchFollowersContentBloomKey(BaseDataCollection dc){
        String userID = getUserId(dc);
        if(userID !=null){
            return userID+"_fetch_followers_bloom";
        }
        return null;
    }
}
