package mx.j2.recommend.util;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.util.*;

import static mx.j2.recommend.util.BaseMagicValueEnum.SCORE_30D;

/**
 * @author xiang.zhou
 * @description
 * @date 2020-08-22
 */
public class ESJsonTool {

    /**
     * 请求ES, 只取结果中的_id信息, 供后续去CA中取详情用
     * *
     */
    public static List<String> loadOnlyIdList(String result) {
        List<String> idList = new ArrayList<>();
        JSONObject responseJson = JSONObject.parseObject(result);
        if (responseJson.containsKey("hits")) {
            JSONObject hitObject = responseJson.getJSONObject("hits");
            if (hitObject.containsKey("hits")) {
                JSONArray hitArray = hitObject.getJSONArray("hits");
                for (int i = 0; i < hitArray.size(); i++) {
                    JSONObject obj = hitArray.getJSONObject(i);
                    if (!obj.containsKey("_id")) {
                        continue;
                    }
                    idList.add(obj.getString("_id"));
                }
            }
        }
        return idList;
    }

    /**
     * 请求ES, 只取结果中的total信息, 目前给
     * *
     */
    public static int loadOnlyTotalNumber(String result) {
        int value = 0;
        JSONObject responseJson = JSONObject.parseObject(result);
        if (responseJson.containsKey("hits")) {
            JSONObject hitObject = responseJson.getJSONObject("hits");
            if (hitObject.containsKey("total")) {
                JSONObject total = hitObject.getJSONObject("total");
                if (total.containsKey("value")) {
                    value = total.getIntValue("value");
                }
            }
        }
        return value;
    }

    /**
     * 请求ES, 取结果中的_id信息和sort信息
     * *
     */
    public static Map<String, String> loadIdSortMap(String result) {
        Map<String, String> map = new LinkedHashMap<>();
        JSONObject responseJson = JSONObject.parseObject(result);
        if (responseJson.containsKey("hits")) {
            JSONObject hitObject = responseJson.getJSONObject("hits");
            if (hitObject.containsKey("hits")) {
                JSONArray hitArray = hitObject.getJSONArray("hits");
                for (int i = 0; i < hitArray.size(); i++) {
                    JSONObject obj = hitArray.getJSONObject(i);
                    if (!obj.containsKey("_id")) {
                        continue;
                    }
                    map.put(obj.getString("_id"), generateNextToken(obj.getJSONArray("sort")));
                }
            }
        }
        return map;
    }

    public static String generateNextToken(JSONArray sortArray) {
        StringBuilder nextToken = new StringBuilder();
        for (int i = 0; i < sortArray.size(); i++) {
            nextToken.append(sortArray.getString(i));
            nextToken.append("|");
        }
        nextToken.deleteCharAt(Math.max(0, nextToken.length()-1));
        return nextToken.toString();
    }

    /**
     * 从es中获取所有你想要的字段，获取的字段可能为null
     * @param result
     * @return
     */
    public static LinkedHashMap<String, JSONObject> loadFieldYouNeedFromEs(String result) {
        LinkedHashMap<String, JSONObject> fieldYouNeedFromEs = new LinkedHashMap<>();

        JSONObject responseJson = JSONObject.parseObject(result);
        if (responseJson.containsKey("hits")) {
            JSONObject hitObject = responseJson.getJSONObject("hits");
            if (hitObject.containsKey("hits")) {
                JSONArray hitArray = hitObject.getJSONArray("hits");
                for (int i = 0; i < hitArray.size(); i++) {
                    JSONObject obj = hitArray.getJSONObject(i);
                    if (!obj.containsKey("_id")) {
                        continue;
                    }
                    JSONObject valueJson = new JSONObject();
                    if (obj.containsKey("_source")) {
                        JSONObject sourceJson = obj.getJSONObject("_source");
                        if (sourceJson.containsKey(SCORE_30D)) {
                            valueJson.put(SCORE_30D, sourceJson.getDoubleValue(SCORE_30D));
                        }
                    }
                    fieldYouNeedFromEs.put(obj.getString("_id"), valueJson);
                }
            }
        }
        return fieldYouNeedFromEs;
    }

    public static List<String> loadOnlyIdList(List<JSONObject> result) {
        List<String> idList = new ArrayList<>();
        for (JSONObject obj : result) {
            if (obj != null && obj.containsKey("_id")) {
                idList.add(obj.getString("_id"));
            }
        }
        return idList;
    }

    /**
     * load 所有的字段
     */
    public static List<JSONObject> loadList(String result) {
        List<JSONObject> idList = new ArrayList<>();
        JSONObject responseJson = JSONObject.parseObject(result);

        if (responseJson.containsKey("hits")) {
            JSONObject hitObject = responseJson.getJSONObject("hits");
            if (hitObject.containsKey("hits")) {
                JSONArray hitArray = hitObject.getJSONArray("hits");
                for (int i = 0; i < hitArray.size(); i++) {
                    JSONObject obj = hitArray.getJSONObject(i);
                    JSONObject source = obj.getJSONObject("_source");
                    //剥离外层
                    source.put("_id", obj.getString("_id"));
                    source.put("_type", obj.getString("_type"));
                    source.put("_index", obj.getString("_index"));
                    source.put("metadata_id", obj.getString("_id"));
                    idList.add(source);
                }
            }
        }
        return idList;
    }

    public static List<JSONObject> loadPoolIndexList(String result) {
        List<JSONObject> idList = new ArrayList<>();
        JSONObject responseJson = JSONObject.parseObject(result);

        if (responseJson.containsKey("hits")) {
            JSONObject hitObject = responseJson.getJSONObject("hits");
            if (hitObject.containsKey("hits")) {
                JSONArray hitArray = hitObject.getJSONArray("hits");
                for (int i = 0; i < hitArray.size(); i++) {
                    JSONObject obj = hitArray.getJSONObject(i);
                    JSONObject source = new JSONObject();
                    //剥离外层
                    source.put("_id", obj.getString("_id"));
                    source.put("_index", obj.getString("_index"));
                    idList.add(source);
                }
            }
        }
        return idList;
    }

    //将sort字段放入List里面
    public static List<JSONObject> loadCardList(String result) {
        List<JSONObject> cardList = new ArrayList<>();
        JSONObject responseJson = JSONObject.parseObject(result);

        if (responseJson.containsKey("hits")) {
            JSONObject hitObject = responseJson.getJSONObject("hits");
            if (hitObject.containsKey("hits")) {
                JSONArray hitArray = hitObject.getJSONArray("hits");
                for (int i = 0; i < hitArray.size(); i++) {
                    JSONObject obj = hitArray.getJSONObject(i);
                    JSONObject source = obj.getJSONObject("_source");
                    //剥离外层
                    source.put("_id", obj.getString("_id"));
                    source.put("metadata_id", obj.getString("_id"));
                    cardList.add(source);
                }
            }
        }
        return cardList;
    }

    public static List<JSONObject> loadLiveList(String result) {
        List<JSONObject> liveList = new ArrayList<>();
        JSONObject responseJson = JSONObject.parseObject(result);

        if (responseJson.containsKey("hits")) {
            JSONObject hitObject = responseJson.getJSONObject("hits");
            if (hitObject.containsKey("hits")) {
                JSONArray hitArray = hitObject.getJSONArray("hits");
                for (int i = 0; i < hitArray.size(); i++) {
                    JSONObject obj = hitArray.getJSONObject(i);
                    JSONObject source = obj.getJSONObject("_source");
                    liveList.add(source);
                }
            }
        }
        return liveList;
    }

    public static void main(String[] args) {
        ESJsonTool.loadList("{\"took\":1,\"timed_out\":false,\"_shards\":{\"total\":5,\"successful\":5,\"skipped\":0,\"failed\":0},\"hits\":{\"total\":1,\"max_score\":null,\"hits\":[{\"_index\":\"takatak_cms_liantiao\",\"_type\":\"Banner\",\"_id\":\"5bf68b89b8dc9fce0bfb567750f69120\",\"_score\":null,\"_source\":{\"platform_type\":1,\"tab_id\":\"5bf68b89b8dc9fce0bfb567750f69100\",\"status\":1,\"order\":1,\"language_pack\":[{\"language_id\":\"en\",\"is_default\":1,\"original_thumbnail_urls\":[{\"width\":1080,\"type\":\"\",\"url\":\"pic/upload/test_pic1584588791911.jpg\",\"height\":607.5}],\"thumbnails\":[{\"formats\":[\"jpg\",\"webp\"],\"size\":{\"bigpic\":202002201600},\"name\":\"test_pic1584588791911\",\"pattern\":\"pic/{id}/{lang}/{ratio}/{width}x{height}/{name}.{format}\",\"lang\":\"en\"}]}],\"create_time\":1584588929424,\"update_time\":1597910101568,\"start_time\":1583906725000,\"end_time\":7226562600000,\"content\":{\"hashtag_id\":\"\",\"link\":\"http://takacms-dev.zenmxapps.com:6677/manage/thrid-videos\",\"video_id\":\"\",\"content_type\":\"20\"}},\"sort\":[1]}]}}");
    }
}
