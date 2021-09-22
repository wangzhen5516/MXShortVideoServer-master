package mx.j2.recommend.data_model.data_collection;

import com.alibaba.fastjson.JSONObject;
import com.newrelic.api.agent.Trace;
import mx.j2.recommend.conf.Conf;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_model.flow.RecommendFlow;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.packer.impl.Mx_Recommend_Other_Packer;
import mx.j2.recommend.thrift.*;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXJudgeUtils;
import mx.j2.recommend.util.MXStringUtils;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static mx.j2.recommend.util.BaseMagicValueEnum.NEXT_TOKEN;
import static mx.j2.recommend.util.DefineTool.FlowInterface.*;


/**
 * 数据集合
 *
 * @author zhongren.li
 */
@NotThreadSafe
public class OtherDataCollection extends BaseDataCollection {
    /**
     * 返回给前端的resultIdList纪录
     */
    public List<String> resultIdListRecord;

    public boolean isNeedToSetToFetchFollowersBloom;

    public List<StickerGroup> stickerGroupList;

    public List<Sticker> stickerList;

    /**
     * 构造函数
     */
    public OtherDataCollection() {
        super();
        init();
    }

    /**
     * 初始化函数
     */
    private void init() {
        resultIdListRecord = new ArrayList<>();
        isNeedToSetToFetchFollowersBloom = false;
        stickerGroupList = new ArrayList<>();
        stickerList = new ArrayList<>();
    }


    /**
     * 由于采用了对象池，所以这里用完以后要清理
     */
    public void clean() {
        this.baseClean();
        resultIdListRecord.clear();
        isNeedToSetToFetchFollowersBloom = false;
        stickerGroupList.clear();
        stickerList.clear();
    }

    /**
     * 创建rpc的结果response
     */
    @Trace(dispatcher = true)
    public void generateResponse() {

        if (FETCH_FOLLOW_CARD_1_0.getName().equals(req.interfaceName) && MXJudgeUtils.isNotEmpty(data.result.resultList)) {
            String id;
            for (Result r : data.result.resultList) {
                id = r.publisherInfo.id;
                if (!resultIdListRecord.contains(id)) {
                    data.response.addToResultList(r);
                }
                if (req.num <= data.response.getResultListSize()) {
                    break;
                }
            }
            return;
        }

        if(MX_FETCH_CMS_PUB_CARD_1_0.getName().equals(req.interfaceName) && MXJudgeUtils.isNotEmpty(data.result.resultList)){
            String id;
            Result lastFillResult = null;
            for (int i = 0; (i < data.result.resultList.size()) && (i<req.num); i++) {
                Result r = data.result.resultList.get(i);
                id = r.publisherInfo.id;
                if (!resultIdListRecord.contains(id)) {
                    resultIdListRecord.add(id);
                    data.response.addToResultList(r);
                    resIdList.add(id);
                    lastFillResult = r;
                }
            }
            if(req.num<data.result.resultList.size()){
                if (lastFillResult.isSetInternalUse()) {
                    lastFillResult.internalUse.setNextToken(lastFillResult.id);
                } else {
                    InternalUse internalUse = new InternalUse();
                    internalUse.setNextToken(lastFillResult.id);
                    lastFillResult.setInternalUse(internalUse);
                }
                setNextToken(lastFillResult);
            }
            return;
        }

        // 添加样式
        if (MXJudgeUtils.isNotEmpty(data.result.resultList)) {
            String id;
            Result lastFillResult = null;

            for (int i = 0; i < data.result.resultList.size(); i++) {
                Result r = data.result.resultList.get(i);
                id = getId(r);
                if (MXJudgeUtils.isNotEmpty(r.attachContent)) {
                    StringBuilder attachContent = new StringBuilder(r.attachContent);
                    attachContent.deleteCharAt(attachContent.length() - 1);
                    attachContent.append(",\"index\":").append(i + 1).append(",");
                    attachContent.append("\"log_id\":\"").append(req.logId).append("\"}");
                    r.setAttachContent(attachContent.toString());
                }

                if (!resultIdListRecord.contains(id)) {
                    resultIdListRecord.add(id);
                    data.response.addToResultList(r);
                    resIdList.add(id);
                    lastFillResult = r;
                }
                // 如果数据已填满，跳出循环
                if (req.num <= data.response.getResultListSize()) {
                    break;
                }
            }

            /*
             * 设置 next token
             */

            int resultListSize = data.response.getResultListSize();// 当前返回给 API 的结果数量

            // 如果返回数据已填满，且还有剩余，则表示肯定还有数据，正常填入 next token
            if (resultListSize == req.num && data.result.resultList.size() > resultListSize || DefineTool.FlowInterface.MX_VIDEOS_OF_THE_TAG_VERSION_1_0.getName().equals(this.req.interfaceName)) {
                fillNextTokenSafe(lastFillResult);
            } else {// 如果可能还有数据，尝试填入 next token
                fillNextTokenIfMayHasNext();
            }
        }

        if (MXJudgeUtils.isNotEmpty(stickerGroupList)) {
            data.response.stickerGroupList = stickerGroupList;
        }

        if (MXJudgeUtils.isNotEmpty(stickerList)) {
            data.response.stickerList = stickerList;
        }

        if (this.status != null) {
            data.response.setStatus(this.status);
        }
        if (null != req.getLogId())
            data.response.setLogId(req.getLogId());
        else if (isDebugModeOpen) {
            if (null == debug.deletedRecordMap) return;
            JSONObject json = new JSONObject(16, true);
            List<Map.Entry<String, Integer>> list = new ArrayList<>(debug.deletedRecordMap.entrySet());
            list.sort((o1, o2) -> o2.getValue() - o1.getValue());
            for (Map.Entry<String, Integer> entry : list) {
                json.put(entry.getKey(), entry.getValue());
            }
            data.response.setLogId(json.toString());
        }
    }

    /**
     * 如果数据库中可能还有数据，则填入 next token
     */
    private void fillNextTokenIfMayHasNext() {
        if (mayHasNext(lastRecallResult)) {
            fillNextTokenSafe(lastRecallResult);
        }
    }

    /**
     * 填写 next token
     */
    private void fillNextTokenSafe(Result lastResult) {
        try {
            setNextToken(lastResult);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 可能数据库中还有数据
     */
    private boolean mayHasNext(Result lastResult) {
        // 发布者作品接口
        if (MX_VIDEOS_OF_THE_PUBLISHER_VERSION_1_0.getName().equals(this.req.interfaceName)
                || MX_VIDEOS_OF_THE_PUBLISHER_ME_VERSION_1_0.getName().equals(this.req.interfaceName)) {
            // 如果本次召回数量大于等于（实际最多只能等于）预定的召回数量，说明可能数据库中还有数据
            if (recallSize >= DefineTool.Recall.Config.SizeEnum.VIDEOS_OF_PUBLISHER.configValue) {
                return true;
            }
        } else if (NEARBY_PEOPLE_VERSION_1_0.getName().equals(this.req.interfaceName)) {
            //相当于无论如何near_by接口都会走setNextToken接口，如果召回的数量不满足预定的召回数量，说明es已被遍历完，设置一个特殊标示
            //在下次请求时从头遍历es数据
            try {
                if (recallSize < DefineTool.Recall.Config.SizeEnum.VIDEOS_OF_PUBLISHER.configValue) {
                    String nextToken = lastResult.internalUse.nextToken;
                    String[] tokens = MXStringUtils.split(nextToken, "|");
                    String newToken = String.format("%s|%s|%s|%s|%s", tokens[0], tokens[1], tokens[2], NEXT_TOKEN, this.radius);
                    lastResult.internalUse.setNextToken(newToken);
                }
                setNextToken(lastResult);
                return false;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // 其他接口暂时不管，走原来的逻辑
        return false;
    }

    /**
     * 返回结果实体对应的 ID
     */
    private String getId(Result result) {
        String resourceType = result.getResultType();
        String id = "";
        if (DefineTool.CategoryEnum.LIVE_STREAM.getName().equals(resourceType)) {
            return result.getLiveStream().getStreamId();
        }
        id = result.getId();
        if (DefineTool.CategoryEnum.SHORT_VIDEO.getName().equals(resourceType)) {
            id = result.getShortVideo().getId();
        }
        return id;
    }

    /**
     * 设置下次请求令牌
     *
     * @param lastResult     当前返回的最后一个结果文档
     */
    private void setNextToken(Result lastResult) {
        if (lastResult.internalUse != null && MXJudgeUtils.isNotEmpty(lastResult.internalUse.nextToken)) {
            data.response.setNextToken(lastResult.internalUse.nextToken);
        }
    }

    @Override
    Result packResult(BaseDocument document) {
        return Mx_Recommend_Other_Packer.packResult(this, document, null);
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

        if (MXJudgeUtils.isNotEmpty(req.userInfo.userId)) {
            this.client.user.userId = req.userInfo.userId;
        } else {
            this.client.user.userId = "nullUserId";
        }

        if (req.num > 30 && !req.interfaceName.equals(MX_VIDEOS_OF_THE_TAG_VERSION_1_0.getName())) {
            req.num = 30;
        }

        if (req.num == 0) {
            req.num = Conf.getDefaultRequestNumber();
        }

        // TODO-WZD temp dirty logic。后期修改 翻页 以后一定要把这段逻辑下掉   @dong.ge
        if ("mx_music_playlist_card_content_version_1_0".equals(req.interfaceName)
                || "mx_music_track_card_content_version_1_0".equals(req.interfaceName)
                || "mx_music_playlist_content_version_1_0".equals(req.interfaceName)) {
            req.setNum(100);
        }

        return true;
    }
}
