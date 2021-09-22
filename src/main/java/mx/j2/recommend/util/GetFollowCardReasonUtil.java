package mx.j2.recommend.util;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import mx.j2.recommend.hystrix.GetFollowCardReasonHttpCommand;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * @author DuoZhao
 * @Date 2021/03/18
 * @Description 获取Follow Card原因的工具
 */
public class GetFollowCardReasonUtil {
    private final static Logger logger = LogManager.getLogger(GetFollowCardReasonUtil.class);

    public static Map<String, String> getReason(String userId, List<String> publisherIds) {
        String result = new GetFollowCardReasonHttpCommand(userId, publisherIds).execute();

        if (MXStringUtils.isBlank(result)) {
            return null;
        }
        Map<String, String> map = new LinkedHashMap<>();
        try {
            // POST请求文档可参见 GetFollowCardReasonHttpCommand 中的接口文档
            JSONObject object = JSONObject.parseObject(result);
            if (object.containsKey("list")) {
                JSONArray items = object.getJSONArray("list");
                for (int i = 0; i < items.size(); i++) {
                    JSONObject item = items.getJSONObject(i);
                    if (item.containsKey("user_id") && item.containsKey("follow_by")) {
                        String publisherId = item.getString("user_id");
                        JSONArray followByIds = item.getJSONArray("follow_by");
                        List<String> followByList = followByIds.toJavaList(String.class);
                        if (MXJudgeUtils.isNotEmpty(followByList)) {
                            map.put(publisherId, followByList.get(0));
                        } else {
                            map.put(publisherId, "");
                        }
                    }
                }
            }
        } catch (Exception e) {
            logger.error("GetFollowCardReasonUtil is error", e);
        }
        return map;
    }
}
