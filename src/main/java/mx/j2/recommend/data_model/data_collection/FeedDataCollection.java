package mx.j2.recommend.data_model.data_collection;

import com.newrelic.api.agent.NewRelic;
import com.newrelic.api.agent.Trace;
import mx.j2.recommend.conf.Conf;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_model.flow.RecommendFlow;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.manager.MXManager;
import mx.j2.recommend.packer.impl.Mx_Recommend_Other_Packer;
import mx.j2.recommend.thrift.ExtraClientInfo;
import mx.j2.recommend.thrift.Request;
import mx.j2.recommend.thrift.Result;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXJudgeUtils;
import mx.j2.recommend.util.MXStringUtils;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static mx.j2.recommend.util.BaseMagicValueEnum.*;


/**
 * 数据集合
 *
 * @author zhongren.li
 */
@NotThreadSafe
public class FeedDataCollection extends BaseDataCollection {
    /**
     * 返回给前端的resultIdList纪录
     */
    public List<String> resultIdListRecord;

    public boolean isNeedToSetToFetchFollowersBloom;

    /**
     * 本次数据是否为保底数据
     */
    public boolean isFallback;

    /**
     * 临时开辟的 给tag偏好的 小流量试验标记
     */
    public String tagPoolSmallFlow;

    /**
     * 构造函数
     */
    public FeedDataCollection() {
        super();
        init();
    }

    /**
     * 初始化函数
     */
    private void init() {
        resultIdListRecord = new ArrayList<>();
        isNeedToSetToFetchFollowersBloom = false;
        tagPoolSmallFlow = STRING_INITIAL_VALUE;
    }


    /**
     * 由于采用了对象池，所以这里用完以后要清理
     */
    public void clean() {
        this.baseClean();
        resultIdListRecord.clear();
        isNeedToSetToFetchFollowersBloom = false;
        isFallback = false;
        tagPoolSmallFlow = STRING_INITIAL_VALUE;
    }

    /**
     * 提前 HighPriorityManualForHotRecall HighPriorityManualForStatusRecall中的数据
     * 并把他们中的UGC视频，尽量分散
     */
    private void publisherDiversity() {
        Set<String> publisherSet = new HashSet<>();
        List<Result> publisherDiversityList = new ArrayList<>();
        int num = req.num;
        int ugcNum = 0;

        for (int i = 0; i < data.result.resultList.size(); i++) {
            Result r = data.result.resultList.get(i);
            // 如果是置顶的视频, 跳过打散
            String recallName = "";
            if (null != r.internalUse && r.isSetInternalUse() && r.internalUse.isSetRecallName()) {
                recallName = r.internalUse.recallName;
            }
            if (!publisherSet.contains(r.internalUse.publisherId) ||
                    recallName.contains("HighPriorityManualForHotRecall") ||
                    recallName.contains("HighPriorityManualForStatusRecall")) {
                publisherDiversityList.add(r);
                publisherSet.add(r.internalUse.publisherId);
                if (r.internalUse.isSetVideoSource() && r.internalUse.getVideoSource().equals(UGC_TAG)) {
                    ugcNum++;
                }

                if (publisherDiversityList.size() >= num) {
                    break;
                }
            }
        }

        // UGC 内容尽可能散开一些
        // 如果是首次请求, 不混入trendingList
        if (1 < ugcNum && num > ugcNum && null != historyIdList && 0 < userHistorySize) {
            List<Result> ugcList = new ArrayList<>();
            for (Result r : publisherDiversityList) {
                if (r.internalUse.isSetVideoSource() && r.internalUse.getVideoSource().equals(UGC_TAG)) {
                    ugcList.add(r);
                }
            }
            publisherDiversityList.removeAll(ugcList);
            int step = num / ugcNum - 1;
            if (MXJudgeUtils.isNotEmpty(publisherDiversityList)) {
                for (int x = ugcNum - 1; x >= 0 && step > 0; x--) {
                    publisherDiversityList.add(Math.min(step * x + 1, publisherDiversityList.size() - 1), ugcList.get(x));
                }
            }
            // 把剩下的怼在publisherDiversityList最后面
            ugcList.removeAll(publisherDiversityList);
            publisherDiversityList.addAll(ugcList);
        }

        if (MXJudgeUtils.isNotEmpty(publisherDiversityList)) {
            data.result.resultList.removeAll(publisherDiversityList);
            data.result.resultList.addAll(0, publisherDiversityList);
        }
    }

    /**
     * 创建rpc的结果response
     */
    @Trace(dispatcher = true)
    public void generateResponse() {
        if (util.cacheStatus == DefineTool.Cache.CacheStatus.IgnoreAll) {
            data.result.resultList.clear();
            data.result.resultList.addAll(cachedResultList);
        }

        // 因上实时召回置顶 ruler 的实验，暂时先注释掉
//        if (NEED_PUBLISHER_DIVERSITY_INTERFACE.equals(req.getInterfaceName())) {
//            publisherDiversity();
//        }

        if (MXJudgeUtils.isNotEmpty(highPriorityVideoForNewUserResultList)) {
            data.result.resultList.addAll(0, highPriorityVideoForNewUserResultList);
        }

        if (MXJudgeUtils.isNotEmpty(highPriorityManualResultList)) {
            data.result.resultList.addAll(0, highPriorityManualResultList);
        }

        // 添加样式
        if (data.result.resultList != null) {
            String id = "";
            int index = 1;
            int total = getTotalNumber();
            List<Result> topList = new ArrayList<>();
            data.result.resultListSize = data.result.resultList.size();
            for (Result r : data.result.resultList) {
                String resourceType = r.getResultType();
                id = "";
                if (DefineTool.CategoryEnum.SHORT_VIDEO.getName().equals(resourceType)) {
                    id = r.getShortVideo().getId();
                }

                if (!resultIdListRecord.contains(id)) {
                    resultIdListRecord.add(id);
                    data.response.addToResultList(r);
                    //置顶单独
                    if (r.internalUse.getIsTophot() == 1) {
                        topHotIdList.add(id);
                    } else {
                        if (r.internalUse.recallName.contains("PoolRecall")) {
                            notTopHotIdList.add(id);
                        }
                    }
                    resIdList.add(id);
                }

                if (MXJudgeUtils.isNotEmpty(r.attachContent)) {
                    StringBuilder attachContent = new StringBuilder(r.attachContent);
                    attachContent.deleteCharAt(attachContent.length() - 1);
                    attachContent.append(",\"index\":").append(index).append(",");
                    attachContent.append("\"log_id\":\"").append(req.logId).append("\"}");
                    r.setAttachContent(attachContent.toString());
                }

                if (MXStringUtils.isNotEmpty(r.debugInfo)) {
                    StringBuilder sb = new StringBuilder(r.debugInfo);
                    r.setDebugInfo(sb.append("\nindex: ").append(index).append(" / ").append(total).toString());
                }
                if (r.internalUse.recallName.contains("NewLanguageRecall")) {
                    topList.add(r);
                }
                if (req.num <= data.response.getResultListSize()) {
                    break;
                }
                index++;
            }

            if (MXJudgeUtils.isNotEmpty(topList)) {
                data.response.getResultList().removeAll(topList);
                data.response.getResultList().addAll(0, topList);
            }

            data.response.setNextToken(id);

            // 将trending页面用户所观看视频的publisherID发送到SNS
//            if (DefineTool.FlowInterface.MX_HOT_TAB_INTERNAL_VERSION_2_0.getName().equals(req.interfaceName)
//                    ||DefineTool.FlowInterface.MX_HOT_TAB_INTERNAL_FOR_MAIN_VERSION_2_0.getName().equals(req.interfaceName)) {
//                DataSourceManager.INSTANCE.getaWSSnsDataSource().getPublisherHistory(this);
//            }

            int i = 2;
            boolean isAllFail = true;
            while (i-- > 0) {
                boolean isSuccess = MXDataSource.redis().setUserRecommendHistoryList(this);
                if (isSuccess) {
                    isAllFail = false;
                    break;
                }
            }

            i = 2;
            while (i-- > 0) {
                boolean isSuccess = MXDataSource.redis().setUserRecommendHistoryListNew(this);
                if (isSuccess) {
                    isAllFail = false;
                    break;
                }
            }

            i = 2;
            while (i-- > 0) {
                boolean isSuccess = MXDataSource.redis().setUserRecommendHistoryListOnlyTopHot(this);
                if (isSuccess) {
                    isAllFail = false;
                    break;
                }
            }

            i = 2;
            while (i-- > 0) {
                boolean isSuccess = MXDataSource.redis().setUserRecommendHistoryListNotTopHot(this);
                if (isSuccess) {
                    isAllFail = false;
                    break;
                }
            }
            //如果都失败，就记录，并只保留一个result
            if (isAllFail) {
                NewRelic.noticeError("ShortHistoryList is fail");
                data.response.resultList = data.response.resultList.subList(0, 1);
            }

//            DataSourceManager.INSTANCE.getReBloomDataSource().setBloomHistoryList(this);
//            MXDataSource.guavaBloom().setBloomFilter(this);

            if (isNeedToSetToFetchFollowersBloom) {
                MXDataSource.rebloom().setFetchFollersBloomHistoryList(this);
            }
        }

        if (null != req.getLogId()) {
            data.response.setLogId(req.getLogId());
        }

        /**
         * 准备好需要往redis上存的结果列表
         */
        if (util.cacheStatus != DefineTool.Cache.CacheStatus.IgnoreAll
                && null != data.response
                && null != data.response.resultList) {
            // 如果是第一次拉取, 不存缓存, 防止用户选择语言
            if (null != historyIdList && NewUserWatchNumber < userHistorySize) {
                cachedResultList.clear();
                data.result.resultList.removeAll(data.response.resultList);
                cachedResultList.addAll(data.result.resultList);
                try {
                    MXManager.writeCache().process(this);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 将请求中的信息置到dc中
     */
    @Trace(dispatcher = true)
    public boolean loadRequest(Request req) {
        if (null == req) {
            this.req = BaseDataCollection.EMPTY_REQUEST;
            return false;
        }
        this.req = req;

        if (MXJudgeUtils.isNotEmpty(req.isDebugModeOpen)) {
            try {
                isDebugModeOpen = Boolean.parseBoolean(req.isDebugModeOpen);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        RecommendFlow flow = null;
        /*debug模式选择绑定flow*/
        if (isDebugModeOpen) {
            flow = MXDataSource.flow()
                    .getRecoFlowByUuId(this, req.getInterfaceName(), req.getUserInfo().getUuid());
        }
        /*此处没开debug模式或者debug模式没拿到走保底*/
        if (flow == null) {
            flow = MXDataSource.flow()
                    .getRecommendFlowByInterfaceName(this, req.getInterfaceName(), req.getUserInfo().getUuid());
        }
        this.recommendFlow = flow;
        if (null == this.recommendFlow) {
            return false;
        }
        String requestType = DefineTool.FlowInterface.findFlowInterfaceByName(req.getInterfaceName(), DefineTool.FlowInterface.DEFAULT).getType();
        if (null == requestType) {
            return false;
        }

        if (MXJudgeUtils.isNotEmpty(req.userInfo.uuid)) {
            this.client.user.uuId = req.userInfo.uuid;
        } else {
            this.client.user.uuId = "nullUuId";
        }

        if (null != req.extraClientInfo) {
            ExtraClientInfo info = req.getExtraClientInfo();
            if (MXStringUtils.isNotEmpty(info.getLastInteractiveId())) {
                realTimeClickIdList.add(info.getLastInteractiveId());
            }
        }

        if (MXJudgeUtils.isNotEmpty(req.userInfo.userId)) {
            this.client.user.userId = req.userInfo.userId;
        } else {
            this.client.user.userId = "nullUserId";
        }

        if (!this.client.user.uuId.equals(this.client.user.userId)) {
            this.client.user.isLogined = true;
        }

        this.client.user.adId = req.userInfo.adId;
        if (MXStringUtils.isNotEmpty(this.client.user.adId)) {
            this.client.user.isHaveMachineID = true;
        }

        if (req.num > 30) {
            req.num = 30;
        }

        if (req.num == 0) {
            req.num = Conf.getDefaultRequestNumber();
        }

        if (MXJudgeUtils.isNotEmpty(flow.requestNum)) {
            try {
                req.num = Integer.parseInt(flow.requestNum);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        return true;
    }

    @Override
    public String dcLog() {
        logMap.put("isFallback", String.valueOf(isFallback));
        return super.dcLog();
    }

    @Override
    Result packResult(BaseDocument document) {
        return Mx_Recommend_Other_Packer.packResult(this, document, null);
    }

    private int getTotalNumber() {
        return Math.min(data.result.resultList.size(), req.num);
    }
}
