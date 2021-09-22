package mx.j2.recommend.stream.impl;

import com.newrelic.api.agent.Trace;
import mx.j2.recommend.data_model.data_collection.InternalDataCollection;
import mx.j2.recommend.data_model.data_collection.OtherDataCollection;
import mx.j2.recommend.manager.InternalManager;
import mx.j2.recommend.manager.ThreadLocalManager;
import mx.j2.recommend.manager.impl.InternalFilterManager;
import mx.j2.recommend.manager.impl.InternalPackerManager;
import mx.j2.recommend.manager.impl.InternalRecallManager;
import mx.j2.recommend.thrift.*;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.LogTool;
import mx.j2.recommend.util.MXJudgeUtils;
import mx.j2.recommend.util.MXStringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * 内部接口推荐流程
 *
 * @author DuoZhao
 * @data 2020/12/08
 */
public class InternalStream extends BaseStream{
    private static Logger logger = LogManager.getLogger(InternalStream.class);
    private List<InternalManager> managerList;
    private DefineTool.StreamEnum stream;
    private final static InternalResponse EMPTY_RESPONSE = new InternalResponse();

    /**
     * Error Message Info
     */
    private static final String ERROR_MESSAGE = "Internal Request Failed, Error Message: %s";
    private static final String ERROR_INTERFACE_NAME = "Internal Interface Name is Invalid";
    private static final String ERROR_RESOURCE_LIST_NULL = "Resource Id List is Invalid";
    private static final String ERROR_UNEXPECTED = "Unexpected Error";
    private static final String ERROR_MAX_REQ_NUM = "Resource Id List Size Should Not Greater Than %s";

    /**
     * 构建request的参数
     */
    private static final String INTERNAL_UUID = "internal-user";
    private static final String INTERNAL_USERID = "internal-user";
    private static final String INTERNAL_AD_ID = "internal-ad-id";
    private static final String INTERNAL_PLATFORM_ID = "internal-platform-id";
    private static final String INTERNAL_RESOURCE_TYPE_FOR_VIDEO_NUM = "online";

    /**
     * 每次请求间隔时间
     */
    private static final int SLEEP_TIME = 1;

    /**
     * 初始化一个otherStream
     */
    private static final OtherStream otherStream = new OtherStream();

    /**
     * 初始化
     */
    @Override
    public void init() {
        stream = DefineTool.StreamEnum.INTERNAL;
        System.out.println("{\"streamInfo\":\"init InternalStream begin\"}");

        managerList = new ArrayList<>();
        managerList.add(InternalRecallManager.INSTANCE);
        managerList.add(InternalFilterManager.INSTANCE);
        managerList.add(InternalPackerManager.INSTANCE);

        logger.info("{\"streamInfo\":\"init InternalStream successfully\"}");
    }

    /**
     * 内部推荐主流程
     *
     * @param request 内部请求
     * @return 内部推荐结果
     */
    @Override
    @Trace(dispatcher = true)
    public InternalResponse internalRecommend(InternalRequest request) {
        InternalDataCollection dc;
        try {
            InternalResponse res = new InternalResponse();
            dc = ThreadLocalManager.getInternalDataCollection();
            ThreadLocalManager.setDC(dc);
            dc.clean();
            ThreadLocalManager.setDC(dc);
            dc.startTime = System.currentTimeMillis();
            if (dc.loadRequest(request)) {
                Long coreProcessStartTime = System.nanoTime();
                // 这个接口不走process, 通过构建request访问已有的线上接口获取数据。目前先这样写，如果以后内部接口多了，再重构。
                if (DefineTool.FlowInterface.INTERNAL_VIDEONUM_OF_THE_PUBLISHER_1_0.getName().equals(dc.internalInterfaceName)) {
                    List<InternalResult> internalResultList = new ArrayList<>();
                    Request requestNormal = internalRequestConvertor(DefineTool.FlowInterface.MX_VIDEONUM_OF_THE_PUBLISHER_VERSION_1_0.getName());
                    requestNormal.setResourceType(INTERNAL_RESOURCE_TYPE_FOR_VIDEO_NUM);
                    for (String resourceId: dc.internalReq.resourceIdList) {
                        requestNormal.resourceId = resourceId;
                        Response responseNormal = otherStream.recommend(requestNormal);
                        if (null != responseNormal) {
                            InternalResult internalResult = new InternalResult();
                            internalResult.setPublisherId(resourceId);
                            internalResult.setNum(responseNormal.resultNum);
                            internalResultList.add(internalResult);
                            // 每次请求sleep 1ms, 以防过度请求对线上环境有影响
                            Thread.sleep(SLEEP_TIME);
                        }
                    }
                    if (null == dc.internalRes.internalResultList) {
                        dc.internalRes.setInternalResultList(new ArrayList<>());
                    }

                    dc.internalRes.internalResultList.addAll(internalResultList);
                } else { // 正常process流程
                    Request req = internalRequestConvertor(dc.internalInterfaceName);
                    OtherDataCollection otherDc = ThreadLocalManager.getOtherDataCollection();
                    ThreadLocalManager.setDC(otherDc);
                    otherDc.clean();
                    otherDc.loadRequest(req);
                    coreProcess(dc, otherDc);
                }
                Long coreProcessEndTime = System.nanoTime();
                dc.appendToTimeRecord(coreProcessEndTime - coreProcessStartTime, "coreProcessTime");
            } else { // 错误信息
                String message;
                if (MXStringUtils.isEmpty(request.interfaceName)){
                    message = String.format(ERROR_MESSAGE, ERROR_INTERFACE_NAME);
                } else if (MXJudgeUtils.isEmpty(request.resourceIdList)) {
                    message = String.format(ERROR_MESSAGE, ERROR_RESOURCE_LIST_NULL);
                } else if (request.getResourceIdListSize() > DefineTool.InternalInterface.valueOf(request.interfaceName.toUpperCase()).getMaxRequestNumber()){
                    message = String.format(ERROR_MESSAGE, String.format(ERROR_MAX_REQ_NUM, DefineTool.InternalInterface.valueOf(request.interfaceName.toUpperCase()).getMaxRequestNumber()));
                } else {
                    message = String.format(ERROR_MESSAGE, ERROR_UNEXPECTED);
                }
                res.errorMessage = message;
                LogTool.printErrorLog(logger, DefineTool.ErrorNoEnum.NORMAL_ERROR.getErrorNo(), message, dc.req, (Object) null);
                return res;
            }
            res = dc.internalRes;
            return res;
        } catch (Exception e) {
            e.printStackTrace();
            String message = String.format("internal request->%s, stream catch exception -> %s" , request.toString(), e.toString());
            LogTool.printErrorLogForInternal(logger, DefineTool.ErrorNoEnum.NORMAL_ERROR.getErrorNo(), message, request, (Object) null);
            return EMPTY_RESPONSE;
        }
    }

    @Override
    public Response setRetryFlag(Request request) {
        return new Response();
    }

    /**
     * 内部接口推荐流程
     *
     * @param dc 请求参数
     * @param otherDc 兼容推荐流
     */
    @Trace(dispatcher = true)
    private void coreProcess(InternalDataCollection dc, OtherDataCollection otherDc) throws Exception {
        for (InternalManager manager : managerList) {
            Long managerStartTime = System.nanoTime();
            manager.process(dc, otherDc);
            Long managerEndTime = System.nanoTime();
            dc.appendToTimeRecord(managerEndTime - managerStartTime, manager.getName());
        }
        Long responseStartTime = System.nanoTime();

        dc.generateResponse();
        Long responseEndTime = System.nanoTime();
        dc.appendToTimeRecord(responseEndTime - responseStartTime, this.getName()+"_responseTime");
    }

    private Request internalRequestConvertor(String interfaceName) {
        Request req = new Request();
        UserInfo userInfo = new UserInfo();
        req.setInterfaceName(interfaceName);
        userInfo.setUuid(INTERNAL_UUID);
        userInfo.setUserId(INTERNAL_USERID);
        userInfo.setAdId(INTERNAL_AD_ID);
        req.setUserInfo(userInfo);
        req.setPlatformId(INTERNAL_PLATFORM_ID);
        return req;
    }
}
