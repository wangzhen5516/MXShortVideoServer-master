package mx.j2.recommend.data_model.data_collection;

import com.alibaba.fastjson.JSONObject;
import com.newrelic.api.agent.Trace;
import mx.j2.recommend.conf.Conf;
import mx.j2.recommend.data_model.flow.RecommendFlow;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.thrift.Banner;
import mx.j2.recommend.thrift.Request;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXJudgeUtils;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


/**
 * 数据集合
 * @author zhongren.li
 */
@NotThreadSafe
public class BannerDataCollection extends BaseDataCollection {
    /**
     * 返回给前端的resultIdList纪录
     */
    public List<String> resultIdListRecord;

    /**
     * bannerList
     */
    public List<Banner> bannerList;

    /**
     * 构造函数
     */
    public BannerDataCollection() {
        super();
        init();
    }

    /**
     * 初始化函数
     */
    private void init() {
        resultIdListRecord = new ArrayList<>();
        bannerList = new ArrayList<>();
    }


    /**
     * 由于采用了对象池，所以这里用完以后要清理
     */
    public void clean() {
        this.baseClean();
        resultIdListRecord.clear();
        bannerList.clear();
    }

    /**
     * 创建rpc的结果response
     */
    @Trace(dispatcher = true)
    public void generateResponse() {

        // 添加样式
        if (bannerList != null) {
            String id;
            for (Banner r : bannerList) {
                id = r.getBannerId();

                if (!resultIdListRecord.contains(id)) {
                    resultIdListRecord.add(id);
                    data.response.addToBannerList(r);
                    resIdList.add(id);
                }
                if (req.num <= data.response.getBannerListSize()) {
                    break;
                }
            }
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
        if(isDebugModeOpen){
            flow = MXDataSource.flow()
                    .getRecoFlowByUuId(this,req.getInterfaceName(),req.getUserInfo().getUuid());
        }
        /*此处没开debug模式或者debug模式没拿到走保底*/
        if(flow == null){
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

        if (req.num == 0) {
            req.num = Conf.getDefaultRequestNumber();
        }

        return true;
    }
}
