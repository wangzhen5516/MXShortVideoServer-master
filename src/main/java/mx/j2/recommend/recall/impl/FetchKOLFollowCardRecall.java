package mx.j2.recommend.recall.impl;

import com.alibaba.fastjson.JSONObject;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_source.ElasticCacheSource;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.thrift.PublisherInfo;
import mx.j2.recommend.util.GetFollowCardReasonUtil;
import mx.j2.recommend.util.MXJudgeUtils;
import mx.j2.recommend.util.FilterDislikePublisherIdUtil;
import mx.j2.recommend.util.isFollowUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FetchKOLFollowCardRecall extends BaseRecall<BaseDataCollection> {
    private final Logger logger = LogManager.getLogger(FetchKOLFollowCardRecall.class);

    private final String REDIS_KEY = "kol_publisher_list";
    private final int REQ_NUM = 50;
    private final String NULL_USER_ID = "nullUserId";
    private final int AMOUNT_OF_REASON = 10;
    private final int REDIS_LENGTH = 199;
    private final boolean REASON_FLAG = true; // Follow Reason 的开关，True 为打开，False 为关闭。特殊情况下可以直接改为False关闭Reason的获取

    @Override
    public boolean skip(BaseDataCollection data) {
        return !MXJudgeUtils.isLogin(data);
    }

    @Override
    public void recall(BaseDataCollection dc) {
        ElasticCacheSource elasticCacheSource = MXDataSource.redis();

        List<String> publisherIds = elasticCacheSource.getTopKOLPublisherIds(REDIS_KEY, 0, REDIS_LENGTH);
        if (MXJudgeUtils.isEmpty(publisherIds)) {
            return;
        }

        List<PublisherInfo> publisherInfos;
        if (!NULL_USER_ID.equals(dc.client.user.userId)) {
            // 过滤已经关注的
            List<String> followList = getFollowedList(dc.client.user.userId, publisherIds);
            if (MXJudgeUtils.isNotEmpty(followList)) {
                publisherIds.removeAll(followList);
            }

            if (MXJudgeUtils.isEmpty(publisherIds)) {
                return;
            }

            // 过滤被用户关闭的
            publisherIds = filterDislikeId(dc.client.user.userId, publisherIds);
            if (MXJudgeUtils.isEmpty(publisherIds)) {
                return;
            }

            // 获取Follow Reason
            if (REASON_FLAG) {
                int reasonAmount = Math.min(AMOUNT_OF_REASON, publisherIds.size());
                publisherInfos = getFollowReason(dc.client.user.userId, publisherIds, reasonAmount);
            }
        } else { // 没登录按照匿名用户，直接返回所有不带reason的数据
            publisherInfos = constructPublisherInfo(publisherIds);
        }

        if (MXJudgeUtils.isEmpty(publisherInfos)) {
            return;
        }

        dc.req.num = REQ_NUM;
        dc.followCardKOLIds.addAll(publisherInfos);
        dc.syncSearchResultSizeMap.put(this.getName(), publisherInfos.size());
    }

    /**
     * 获取已经关注的publisher id
     *
     * @param userId
     * @param publisherIds 最多200个publisher id
     * @return 关注的publisher id
     */
    private List<String> getFollowedList(String userId, List<String> publisherIds) {
        List<String> followedIds = new ArrayList<>();
        try {
            followedIds = isFollowUtil.getFollowedIdsPostRequest(userId, publisherIds);
        } catch (Exception e) {
            logger.error("getFollowedIds(FetchFollowCardRecall) is error from httpRequest", e);
        }
        return followedIds;
    }

    /**
     * 过滤被用户 Dislike 的 publisher
     *
     * @param userId
     * @param publisherIds 需要过滤的publisher id
     * @return 过滤后的publisher id list
     */
    private List<String> filterDislikeId(String userId, List<String> publisherIds) {
        List<String> list = new ArrayList<>();
        try {
            list = FilterDislikePublisherIdUtil.filterId(userId, publisherIds);
        } catch (Exception e) {
            logger.error("filterDislikeId(FetchFollowCardRecall) is error from httpRequest", e);
        }
        return list;
    }

    /**
     * 获取publisher id 的 follow reason, follow reason是publisher id，具体信息由API组装。如果HTTP请求熔断，则放弃获取reason将所有publisher打包成PublisherInfo
     *
     * @param userId
     * @param publisherIds 所有的publisher id
     * @param amount       由于Search的HTTP请求算力不够，只提供固定个数的reason
     * @return
     */
    private List<PublisherInfo> getFollowReason(String userId, List<String> publisherIds, int amount) {
        List<PublisherInfo> infos = new ArrayList<>();
        try {
            Map<String, String> map = GetFollowCardReasonUtil.getReason(userId, publisherIds.subList(0, Math.min(amount, publisherIds.size())));
            if (MXJudgeUtils.isEmpty(map)) { // 获取reason 失败，构建没有reason的publisherInfo返回，保证至少卡片不会不显示
                infos = constructPublisherInfo(publisherIds);
            } else {
                infos = constructPublisherInfo(map);
                if (publisherIds.size() > amount) {
                    List<PublisherInfo> noReasonInfos = constructPublisherInfo(publisherIds.subList(amount, publisherIds.size()));
                    infos.addAll(noReasonInfos);
                }
            }
        } catch (Exception e) {
            logger.error("getFollowReason(FetchFollowCardRecall) is error from httpRequest", e);
        }
        return infos;
    }

    /**
     * 将没有reason的publisher id构建为PublisherInfo
     *
     * @param publisherIds
     * @return List of PublisherInfo
     */
    private List<PublisherInfo> constructPublisherInfo(List<String> publisherIds) {
        List<PublisherInfo> publisherInfos = new ArrayList<>();
        for (String publisherId : publisherIds) {
            PublisherInfo publisherInfo = new PublisherInfo();
            JSONObject object = new JSONObject();
            object.put("name", "");
            publisherInfo.setId(publisherId);
            publisherInfo.setReason(object.toJSONString());
            publisherInfos.add(publisherInfo);
        }
        return publisherInfos;
    }

    /**
     * 将有reason的publisher id构建为PublisherInfo
     *
     * @param rawInfoMap key: publisher id, value: reason(publisher id, the detail of follow reason will be constructed by API)
     * @return List of PublisherInfo
     */
    private List<PublisherInfo> constructPublisherInfo(Map<String, String> rawInfoMap) {
        List<PublisherInfo> publisherInfos = new ArrayList<>();
        for (Map.Entry<String, String> entry : rawInfoMap.entrySet()) {
            PublisherInfo publisherInfo = new PublisherInfo();
            JSONObject object = new JSONObject();
            object.put("name", entry.getValue());
            publisherInfo.setId(entry.getKey());
            publisherInfo.setReason(object.toJSONString());
            publisherInfos.add(publisherInfo);
        }
        return publisherInfos;
    }
}