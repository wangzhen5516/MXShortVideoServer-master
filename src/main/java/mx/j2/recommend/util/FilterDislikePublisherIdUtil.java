package mx.j2.recommend.util;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import mx.j2.recommend.hystrix.FilterDislikePublisherHttpCommand;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * @author DuoZhao
 * @Date 2021/03/18
 * @Description 过滤被用户dislike的publisher的工具
 */
public class FilterDislikePublisherIdUtil {
    private final static Logger logger = LogManager.getLogger(FilterDislikePublisherIdUtil.class);

    public static List<String> filterId(String userId, List<String> publisherIds) {
        String result = new FilterDislikePublisherHttpCommand(userId, publisherIds).execute();
        if (MXStringUtils.isBlank(result)) {
            return null;
        }
        List<String> filteredId = new ArrayList<>();
        try {
            JSONObject object = JSONObject.parseObject(result);
            if (object.containsKey("list")) {
                JSONArray items = object.getJSONArray("list");
                for (int i = 0; i < items.size(); i++) {
                    JSONObject item = items.getJSONObject(i);
                    if (item.containsKey("user_id")) {
                        filteredId.add(item.getString("user_id"));
                    }
                }
            }
        } catch (Exception e) {
            logger.error("FilterDislikePublisherIdUtil is error", e);
        }
        return filteredId;
    }

}
