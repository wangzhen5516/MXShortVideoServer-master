package mx.j2.recommend.recall.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_source.LocalCacheDataSource;
import mx.j2.recommend.data_source.StrategyCassandraDataSource;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.MXJudgeUtils;
import mx.j2.recommend.util.MXStringUtils;
import mx.j2.recommend.util.isFollowUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * @author qiqi
 * @date 2020-12-19 17:22
 */
public class SimilarFollowersRecall extends BaseRecall<BaseDataCollection> {
    Logger logger = LogManager.getLogger(SimilarFollowersRecall.class);
    private static final int MAX_REQ_NUM = 30;

    // IPL 联赛机构号
    private List<String> IPL_PUBLISHERS = new ArrayList<String>() {
        {
            add("12106400860101820302930");
            add("12108233826351981934640");
            add("12101891698786515667883");
            add("12113072240131530626730");
            add("12108058617796472309817");
            add("12112952049452806153931");
        }
    };

    @Override
    public boolean skip(BaseDataCollection dc) {
        if (dc.req == null || dc.req.userInfo == null) {
            return true;
        }
        String publisherId = dc.req.getResourceId();
        String userId = dc.req.userInfo.userId;
        return MXStringUtils.isBlank(publisherId) || MXStringUtils.isBlank(userId) || !MXJudgeUtils.isLogin(dc);
    }

    @Override
    public void recall(BaseDataCollection dc) {
        String publisherId = dc.req.getResourceId();
        String userId = dc.req.userInfo.userId;
        String localKey = String.format("%s_%s", publisherId, this.getName());

        StrategyCassandraDataSource strategyCaDataSource = MXDataSource.strategyCA();
        LocalCacheDataSource localCacheDataSource = MXDataSource.cache();
        List<String> relateIds = localCacheDataSource.getSimilarPublisherIdsCache(localKey);
        if (MXJudgeUtils.isEmpty(relateIds)) {
            relateIds = strategyCaDataSource.getSimilarFollowersIds(publisherId);
            localCacheDataSource.setSimilarPublisherIdsCache(localKey, relateIds);
        }

        // TODO-WZD 脏逻辑：临时插入 IPL 相关账号
        if (IPL_PUBLISHERS.contains(publisherId)) {
            if (relateIds == null) {
                relateIds = new ArrayList<>();
            }

            // 要插入的 ID
            List<String> insertPublishers = new ArrayList<>(IPL_PUBLISHERS);
            insertPublishers.remove(publisherId);

            // 先从推荐列表中删除已存在的 IPL 账号
            relateIds.removeAll(IPL_PUBLISHERS);

            // 再插入到头部
            relateIds.addAll(0, insertPublishers);
        }

        if (MXJudgeUtils.isEmpty(relateIds)) {
            logger.error(String.format("have no result from SimilarFollowersRecall，Id:%s", publisherId));
            return;
        }
        if (relateIds.size() > MAX_REQ_NUM) {
            relateIds = relateIds.subList(0, MAX_REQ_NUM);
        }
        StringBuilder buffer = new StringBuilder();
        for (int i = 0; i < relateIds.size(); ++i) {
            if (i == relateIds.size() - 1) {
                buffer.append(relateIds.get(i));
            } else {
                buffer.append(relateIds.get(i));
                buffer.append(",");
            }
        }
        List<String> followedIds = new ArrayList<>();
        /*获取已经关注的列表*/
        try {
            followedIds = getFollowedIds(userId, buffer.toString());
        } catch (Exception e) {
            logger.error("getFollowedIds is error from httpRequest", e);
        }
        List<String> resultIds = new ArrayList<>(relateIds);
        if (MXJudgeUtils.isNotEmpty(followedIds)) {
            /*清除已经关注的*/
            resultIds.removeAll(followedIds);
        }
        dc.data.response.setPublisherIds(resultIds);
        dc.syncSearchResultSizeMap.put(this.getName(), resultIds.size());
    }

    /**
     * @return
     */
    private List<String> getFollowedIds(String userId, String publisherIds) {
        return isFollowUtil.getFollowedIds(userId, publisherIds);
    }
}
