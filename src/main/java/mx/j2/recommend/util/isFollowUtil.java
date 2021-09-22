package mx.j2.recommend.util;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import mx.j2.recommend.hystrix.IsFollowHttpCommand;
import mx.j2.recommend.hystrix.IsFollowPostHttpCommand;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * @author qiqi
 * @date 2020-12-28 11:11
 */
public class isFollowUtil {

    public static Logger logger = LogManager.getLogger(isFollowUtil.class);

    public isFollowUtil() {

    }

    public static List<String> getFollowedIds(String userId, String publisherIds) {
        if (MXStringUtils.isBlank(userId) || MXStringUtils.isBlank(publisherIds)) {
            return null;
        }

        String result = new IsFollowHttpCommand(userId, publisherIds).execute();
        if (MXStringUtils.isBlank(result)) {
            return null;
        }
        List<String> followedIds = new ArrayList<>();
        try {
            JSONObject object = JSONObject.parseObject(result);
            if (object.containsKey("list")) {
                followedIds = JSONArray.parseArray(object.getString("list")).toJavaList(String.class);
            }
        } catch (Exception e) {
            logger.error("getFollowedIds is error", e);
        }
        return followedIds;
    }

    /**
     * 获取关注情况的POST请求
     *
     * @param userId
     * @param publisherIds 需要查询的publisher id 最多200个
     * @return 关注的publisher id
     */
    public static List<String> getFollowedIdsPostRequest(String userId, List<String> publisherIds) {
        if (MXStringUtils.isBlank(userId) || MXJudgeUtils.isEmpty(publisherIds)) {
            return null;
        }

        String result = new IsFollowPostHttpCommand(userId, publisherIds).execute();
        if (MXStringUtils.isBlank(result)) {
            return null;
        }
        List<String> followedIds = new ArrayList<>();
        try {
            JSONObject object = JSONObject.parseObject(result);
            if (object.containsKey("list")) {
                followedIds = JSONArray.parseArray(object.getString("list")).toJavaList(String.class);
            }
        } catch (Exception e) {
            logger.error("getFollowedIds is error", e);
        }
        return followedIds;
    }

}
