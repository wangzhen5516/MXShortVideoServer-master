package mx.j2.recommend.util;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.hystrix.IsFriendHttpCommand;

import java.util.ArrayList;
import java.util.List;

/**
 * @author DuoZhao
 * @ Author     ：DuoZhao
 * @ Date       ：Created in 下午2:10 2020/09/16
 * @ Description：获取两个用户是否是好友
 */

public class IsFriendUtil {

    public IsFriendUtil() {
    }

    public static Integer isFriend(String userId, String publisherId) {
        String result = new IsFriendHttpCommand(userId, publisherId).execute();
        Integer status = null;
        if (MXJudgeUtils.isEmpty(result)) {
            return status;
        }
        JSONObject object;
        try {
            object = JSONObject.parseObject(result);
        } catch (Exception e) {
            e.printStackTrace();
            return status;
        }
        if (object != null && object.containsKey("status")) {
            String statusObject = object.getString("status");
            if (null != statusObject) {
                status = Integer.valueOf(statusObject);
            }
        }
        return status;
    }
}
