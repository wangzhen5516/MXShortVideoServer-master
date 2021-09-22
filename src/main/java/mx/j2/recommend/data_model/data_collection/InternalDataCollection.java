package mx.j2.recommend.data_model.data_collection;

import com.newrelic.api.agent.Trace;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_model.flow.RecommendFlow;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.thrift.InternalRequest;
import mx.j2.recommend.thrift.InternalResponse;
import mx.j2.recommend.thrift.InternalResult;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXJudgeUtils;
import mx.j2.recommend.util.MXStringUtils;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.ArrayList;
import java.util.List;

/**
 * 内部接口数据集合
 *
 * @author DuoZhao
 * @date 2020/12/05 11:15AM
 */
@NotThreadSafe
public class InternalDataCollection extends BaseDataCollection {
    public static final DefineTool.StreamEnum STREAM = DefineTool.StreamEnum.INTERNAL;
    public static final InternalRequest EMPTY_REQUEST = new InternalRequest();

    private static final String INTERNAL_UUID = "internal-uuid";
    private static final boolean COMPLETE_RESPONSE_SIGNAL = true;

    /**
     * 内部request
     */
    public InternalRequest internalReq;

    /**
     * 内部接口名称
     */
    public String internalInterfaceName;

    /**
     * 内部response
     */
    public InternalResponse internalRes;

    /**
     * 返回给前端的resultIdList纪录
     */
    public List<String> resultIdListRecord;

    /**
     * 归并到一起的列表，同步结果直接存到这里面。
     */
    public List<BaseDocument> mergedList;

    /**
     * 推荐流
     */
    public RecommendFlow recommendFlow;

    /**
     * 内部result list
     */
    public List<InternalResult> internalResultList;

    /**
     * 构造函数
     */
    public InternalDataCollection() {
        super();
        init();
    }

    /**
     * 初始化函数
     */
    private void init() {
        internalReq = null;
        internalRes = new InternalResponse();
        resultIdListRecord = new ArrayList<>();
        mergedList = new ArrayList<>();
        recommendFlow = new RecommendFlow();
        internalResultList = new ArrayList<>();
    }

    /**
     * 由于采用了对象池，所以这里用完以后要清理
     */
    public void clean() {
        super.baseClean();
        internalReq = null;
        internalRes.clear();
        resultIdListRecord.clear();
        mergedList.clear();
        recommendFlow = null;
        internalResultList.clear();
    }

    @Trace(dispatcher = true)
    public void generateResponse() {
        if (null == internalRes.internalResultList) {
            internalRes.setInternalResultList(new ArrayList<>());
        }
        internalRes.internalResultList.addAll(internalResultList);
        internalRes.setProcessingStatus(COMPLETE_RESPONSE_SIGNAL);
    }

    /**
     * 将请求中的信息置到dc中
     */
    public boolean loadRequest(InternalRequest req) {
        if (null == req) {
            this.internalReq = InternalDataCollection.EMPTY_REQUEST;
            return false;
        }
        this.internalReq = req;

        this.recommendFlow = MXDataSource.flow().getRecommendFlowByInterfaceName(this, req.getInterfaceName(), INTERNAL_UUID);
        if (null == this.recommendFlow) {
            return false;
        }

        if (MXStringUtils.isEmpty(internalReq.interfaceName)) {
            return false;
        }

        if (MXJudgeUtils.isEmpty(internalReq.resourceIdList)) {
            return false;
        }

        if (internalReq.resourceIdList.size() > DefineTool.InternalInterface.valueOf(req.interfaceName.toUpperCase()).getMaxRequestNumber()) {
            return false;
        }

        this.internalInterfaceName = req.interfaceName;
        return true;
    }
}
