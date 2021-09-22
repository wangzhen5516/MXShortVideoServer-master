package mx.j2.recommend.util;

import com.alibaba.fastjson.JSON;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.thrift.Location;
import mx.j2.recommend.thrift.Request;
import mx.j2.recommend.thrift.UserInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * @author ：zhongrenli
 * @date ：Created in 4:28 下午 2021/5/25
 */
public class RequestFormat {
    private static final Logger logger = LogManager.getLogger(RequestFormat.class);

    public static void format(BaseDataCollection dc) {
        try {
            Map<String, Object> map = new HashMap<>(32);
            formatRequest(dc.req, map);
            formatOther(dc, map);
            logger.info(JSON.toJSONString(map));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void formatRequest(Request request, Map<String, Object> map) {
        if (!Optional.ofNullable(request).isPresent()) {
            return;
        }
        OptionalUtil.ofNullable(request)
                .getUtil(Request::getOriginalInterfaceName)
                .ifPresent(v -> map.put("ori_interface", v));
        OptionalUtil.ofNullable(request)
                .getUtil(Request::getInterfaceName)
                .ifPresent(v -> map.put("interface", v));
        OptionalUtil.ofNullable(request)
                .getUtil(Request::getUserInfo)
                .getUtil(UserInfo::getUuid)
                .ifPresent(v -> map.put("uuid", v));
        OptionalUtil.ofNullable(request)
                .getUtil(Request::getUserInfo)
                .getUtil(UserInfo::getUserId)
                .ifPresent(v -> map.put("user_id", v));
        OptionalUtil.ofNullable(request)
                .getUtil(Request::getLogId)
                .ifPresent(v -> map.put("log_id", v));
        OptionalUtil.ofNullable(request)
                .getUtil(Request::getResourceId)
                .ifPresent(v -> map.put("r_Id", v));
        OptionalUtil.ofNullable(request)
                .getUtil(Request::getResourceType)
                .ifPresent(v -> map.put("r_type", v));
        OptionalUtil.ofNullable(request)
                .getUtil(Request::getClientVersion)
                .ifPresent(v -> map.put("c_version", v));
        OptionalUtil.ofNullable(request)
                .getUtil(Request::getLocation)
                .getUtil(Location::getCountry)
                .ifPresent(v -> map.put("country", v));
        OptionalUtil.ofNullable(request)
                .getUtil(Request::getLocation)
                .getUtil(Location::getCity)
                .ifPresent(v -> map.put("city", v));
        OptionalUtil.ofNullable(request)
                .getUtil(Request::getLocation)
                .getUtil(Location::getState)
                .ifPresent(v -> map.put("state", v));
        OptionalUtil.ofNullable(request)
                .getUtil(Request::getLanguageList)
                .ifPresent(v -> map.put("language", v));
    }

    private static void formatOther(BaseDataCollection dc, Map<String, Object> map) {
        OptionalUtil.ofNullable(dc.recallFilterCount)
                .ifPresent(v -> {
                    Map<String, Integer> m = new HashMap<>();
                    for (String key : v.elementSet()) {
                        m.put(key, v.count(key));
                    }
                    map.put("recall_deleted", m);
                });
        OptionalUtil.ofNullable(dc.historyIdList)
                .ifPresent(v -> map.put("his_list_size", v.size()));
        OptionalUtil.ofNullable(dc.userHistorySize)
                .ifPresent(v -> map.put("his_bloom_size", v));
        OptionalUtil.ofNullable(dc.data.result.resultList)
                .ifPresent(v -> map.put("result_size", v.size()));
        OptionalUtil.ofNullable(dc.debug.deletedRecordMap)
                .ifPresent(v -> map.put("filter_deleted", simplifyDeletedRecordMap(v)));
        OptionalUtil.ofNullable(dc.syncSearchResultSizeMap)
                .ifPresent(v -> map.put("recall_size", v));
    }

    private static Map<String, Map<String, Integer>> simplifyDeletedRecordMap(Map<String, Integer> oldMap) {
        if (oldMap == null) {
            return null;
        }
        Map<String, Map<String, Integer>> outerMap = new HashMap<>();
        for (Map.Entry<String, Integer> entry : oldMap.entrySet()) {
            String key = entry.getKey();

            String outer = "SINGLE";
            String inner = key;
            if (key != null && key.matches("[\\w]*_lv.*")) {
                outer = key.replaceFirst("_lv.*", "");
                inner = key.replace(outer + "_", "");

            }

            if (outerMap.get(outer) == null) {
                Map<String, Integer> innerMap = new HashMap<>();
                innerMap.put(inner, entry.getValue());
                outerMap.put(outer, innerMap);
            } else {
                outerMap.get(outer).put(inner, entry.getValue());
            }
        }
        return outerMap;
    }


    public static void main(String[] args) {
        String request = "Request(interfaceName:mx_followers_live_bool_ref\n" +
                "resh_version_1_0, userInfo:UserInfo(uuid:de99d28f-7977-4297-a082-fb622078cea0378366867, userId:14918709531894, adId:52f0b08e-943e-49e4-b8fb-4e348807b810), platfor\n" +
                "mId:1, tabId:, num:0, type:0, logId:c2n4ra9v0bddn7p8t690, resourceId:14918709531894, resourceType:publisher, languageList:[], clientVersion:11510, timeSign:1622035881275, originalInterfaceName:mx_followers_live_bool_refresh_version_1_0, lastRefreshTime:1622035355876, location:Location(country:IND, state:Jharkhand, city:Ranchi, coordinateX:25.5610386, coordinateY:83.9693307), oldLanguageList:[], interestTagList:[])";
    }
}
