package mx.j2.recommend.stream.impl;

import com.newrelic.api.agent.Trace;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.data_collection.FeedDataCollection;
import mx.j2.recommend.data_source.FallbackDataSource;
import mx.j2.recommend.manager.IStreamComponentManager;
import mx.j2.recommend.manager.MXManager;
import mx.j2.recommend.manager.ThreadLocalManager;
import mx.j2.recommend.thrift.Request;
import mx.j2.recommend.thrift.Response;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.LogTool;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.concurrent.ThreadSafe;
import java.util.ArrayList;
import java.util.List;

/**
 * 推荐主流程
 *
 * @author zhuowei
 */
@ThreadSafe
public class FeedStream extends BaseStream {
    private static Logger logger = LogManager.getLogger(FeedStream.class);
    private List<IStreamComponentManager> managerList;
    private DefineTool.StreamEnum stream;
    private final static Response EMPTY_RESPONSE = new Response();

    /**
     * 初始化
     */
    @Override
    public void init() {
        stream = DefineTool.StreamEnum.FEED;
        System.out.println("{\"streamInfo\":\"init FeedStream begin\"}");

        managerList = new ArrayList<>();
        //managerList.add(PullCacheManager.INSTANCE);
        //managerList.add(MXManager.preRecall());暂时不需要该组件
        managerList.add(MXManager.prepare());
        managerList.add(MXManager.recall());
        managerList.add(MXManager.filter());
        managerList.add(MXManager.predictor());
        managerList.add(MXManager.scorer());
        managerList.add(MXManager.ranker());
        managerList.add(MXManager.mixer());
        managerList.add(MXManager.packer());
        managerList.add(MXManager.ruler());

        logger.info("{\"streamInfo\":\"init FeedStream successfully\"}");
    }

    /**
     * 推荐主流程
     *
     * @throws Exception
     */
    @Override
    @Trace(dispatcher = true)
    public Response recommend(Request request) throws Exception {
        FeedDataCollection dc = null;
        Request req = request;
        dc = ThreadLocalManager.getFeedDataCollection();
        ThreadLocalManager.setDC(dc);
        dc.clean();
        ThreadLocalManager.setDC(dc);
        dc.startTime = System.currentTimeMillis();
        if (dc.loadRequest(req)) {
            Long coreProcessStartTime = System.nanoTime();
            coreProcess(dc);
            Long coreProcessEndTime = System.nanoTime();
            dc.appendToTimeRecord(coreProcessEndTime - coreProcessStartTime, "coreProcessTime");
        } else {
            String message = String.format("unknown interfaceName, req -> %s", req.toString());
            LogTool.printErrorLog(logger, DefineTool.ErrorNoEnum.NORMAL_ERROR.getErrorNo(), message, dc.req, null);
            return EMPTY_RESPONSE;
        }
        Response res = dc.data.response;
        dc.endTime = System.currentTimeMillis();

        String dcLog = dc.dcLog();
        logger.info(dcLog);
        if (dc.isDebugModeOpen) {
            res.setLogMap(dcLog);
        }

//        RequestFormat.format(dc);
        LogTool.serializedRequest(req.deepCopy());
        return res;
    }

    /**
     * 列表页服务推荐主流程
     *
     * @param dc,rf 请求参数
     * @throws Exception
     */
    @Trace(dispatcher = true)
    private void coreProcess(FeedDataCollection dc) throws Exception {
        for (IStreamComponentManager manager : managerList) {
            Long managerStartTime = System.nanoTime();
            manager.process(dc);
            Long managerEndTime = System.nanoTime();
            dc.appendToTimeRecord(managerEndTime - managerStartTime, manager.getName());
        }
        dc.generateResponse();
    }

    /**
     * 当前是否能够走保底流程
     */
    @Override
    public boolean hasFallback(String interfaceName, boolean force) {
        boolean support = isSupport(interfaceName);
        boolean open = force ? FallbackDataSource.INSTANCE.isForceOpen() : FallbackDataSource.INSTANCE.isOpen();
        return support && open;
    }

    /**
     * feed 流支持保底的接口
     */
    private boolean isSupport(String interfaceName) {
        return DefineTool.FlowInterface.MX_HOT_TAB_INTERNAL_VERSION_2_0.getName().equals(interfaceName)
                || DefineTool.FlowInterface.MX_HOT_TAB_INTERNAL_FOR_MAIN_VERSION_2_0.getName().equals(interfaceName)
                || DefineTool.FlowInterface.MX_HOT_TAB_INTERNAL_VERSION_1_0.getName().equals(interfaceName)
                || DefineTool.FlowInterface.MX_HOT_TAB_INTERNAL_FOR_OPERATE_VERSION_1_0.getName().equals(interfaceName)
                || DefineTool.FlowInterface.MX_STATUS_TAB_INTERNAL_VERSION_1_0.getName().equals(interfaceName)
                || DefineTool.FlowInterface.MX_STATUS_TAB_INTERNAL_FOR_OPERATE_VERSION_1_0.getName().equals(interfaceName);
    }

    /**
     * 保底流程
     */
    @Override
    public Response fallback(Request request) {
        // 先做保底条件检查
        if (!hasFallback(request.interfaceName, false)) {
            return EMPTY_RESPONSE;
        }

        FeedDataCollection dc;

        try {
            dc = ThreadLocalManager.getFeedDataCollection();
            ThreadLocalManager.setDC(dc);
            dc.clean();
            dc.isFallback = true;
            dc.startTime = System.currentTimeMillis();

            if (dc.loadRequest(request)) {
                Long fallbackProcessStartTime = System.nanoTime();

                fallbackProcess(dc);
                dc.generateResponse();

                Long fallbackProcessEndTime = System.nanoTime();
                dc.appendToTimeRecord(fallbackProcessEndTime - fallbackProcessStartTime, "fallbackProcessTime");
            } else {
                String message = String.format("unknown interfaceName, req -> %s", request.toString());
                LogTool.printErrorLog(logger, DefineTool.ErrorNoEnum.NORMAL_ERROR.getErrorNo(), message, dc.req, null);
                return EMPTY_RESPONSE;
            }

            Response res = dc.data.response;
            dc.endTime = System.currentTimeMillis();
            logger.info(dc.dcLog());
            return res;
        } catch (Exception e) {
            e.printStackTrace();
            String message = String.format("request->%s, stream catch exception -> %s", request.toString(), e.toString());
            LogTool.printErrorLog(logger, DefineTool.ErrorNoEnum.NORMAL_ERROR.getErrorNo(), message, request, null);
            return EMPTY_RESPONSE;
        }
    }

    @Override
    public Response setRetryFlag(Request request) {
        Response res = new Response();
        res.setNeedRetry(true);
        return res;
    }

    /**
     * 保底流程
     */
    private void fallbackProcess(BaseDataCollection dc) {
        try {
            MXManager.fallback().process(dc);
            MXManager.packer().process(dc);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}