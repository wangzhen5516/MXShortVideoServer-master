package mx.j2.recommend.recall.impl;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import mx.j2.recommend.conf.Conf;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.LogTool;
import mx.j2.recommend.util.MXJudgeUtils;
import mx.j2.recommend.util.isFollowUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static mx.j2.recommend.util.DefineTool.ErrorNoEnum.NORMAL_ERROR;

/**
 * @author qiqi
 * @date 2021-03-16 15:09
 */
public class GetFollowersLiveBoolRefreshRecall extends BaseRecall<BaseDataCollection> {
    private final Logger logger = LogManager.getLogger(GetFollowersLiveBoolRefreshRecall.class);
    private static final String ES_REQUEST = String.format("/%s/Live/_search?pretty=false", Conf.getCmsIndex());
    private static final int RECALL_SIZE = 200;

    private final String LIVE_CACHE_KEY = "live_bool_refresh_key";
    private final JSONObject BOOL = constructQuery();
    private final JSONArray SORT = constructSort();

    @Override
    public boolean skip(BaseDataCollection dc) {
        return MXJudgeUtils.isEmpty(dc.req.resourceId) || MXJudgeUtils.isEmpty(dc.req.resourceType) || MXJudgeUtils.isEmpty(dc.req.getLastRefreshTime()) || !MXJudgeUtils.isLogin(dc);
    }

    @Override
    public void recall(BaseDataCollection dc) {
        long lastRefreshSecondTime;
        try {
            lastRefreshSecondTime = Long.parseLong(dc.req.lastRefreshTime) / 1000;
        } catch (Exception e) {
            LogTool.printErrorLog(logger, NORMAL_ERROR.getErrorNo(), "error lastRefreshTime", dc.req);
            dc.data.response.setRedDot(false);
            return;
        }

        Map<String, Long> liveUserMap = getLatestLiveMap();
        if (MXJudgeUtils.isEmpty(liveUserMap)) {
            dc.data.response.setRedDot(false);
            return;
        }

        List<String> liveUserList = new ArrayList<>();
        for (Map.Entry<String, Long> entry : liveUserMap.entrySet()) {
            if (entry.getValue() > lastRefreshSecondTime) {
                liveUserList.add(entry.getKey());
            }
        }
        if (MXJudgeUtils.isEmpty(liveUserList)) {
            dc.data.response.setRedDot(false);
            return;
        }

        List<String> followedLiveUserList = isFollowUtil.getFollowedIdsPostRequest(dc.client.user.userId, liveUserList);
        dc.data.response.setRedDot(!MXJudgeUtils.isEmpty(followedLiveUserList));
    }

    /**
     * 获取最新的直播publisher和时间
     *
     * @return 直播人员以及开播时间的Map
     */
    private Map<String, Long> getLatestLiveMap() {
        Map<String, Long> liveUserMap = MXDataSource.cache().getLiveUserCache(LIVE_CACHE_KEY);
        if (MXJudgeUtils.isEmpty(liveUserMap)) {
            liveUserMap = MXDataSource.ES().sendSyncReturnPublisherAndTime(ES_REQUEST, constructContent(BOOL, 0, RECALL_SIZE, null, SORT).toString());
            MXDataSource.cache().setLiveUserCache(LIVE_CACHE_KEY, liveUserMap);
        }
        return liveUserMap;
    }


    private JSONObject constructQuery() {
        JSONObject bool = new JSONObject();
        JSONObject mustObj = new JSONObject();
        JSONArray mustArr = new JSONArray();

        JSONObject statusMatch = new JSONObject();
        JSONObject statusMatchObj = new JSONObject();
        statusMatch.put("status", "1");
        statusMatchObj.put("match", statusMatch);
        mustArr.add(statusMatchObj);

        mustObj.put("must", mustArr);
        bool.put("bool", mustObj);
        return bool;
    }

    private JSONArray constructSort() {
        JSONArray sortJson = new JSONArray();
        JSONObject sortCore = new JSONObject();
        sortCore.put("order", "desc");
        JSONObject sortObj = new JSONObject();
        sortObj.put("start_time", sortCore);
        sortJson.add(sortObj);
        return sortJson;
    }
}
