package mx.j2.recommend.stream.impl;

import com.newrelic.api.agent.Trace;
import mx.j2.recommend.data_model.data_collection.OtherDataCollection;
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
public class OtherStream extends BaseStream {
    private static Logger logger = LogManager.getLogger(OtherStream.class);
    private List<IStreamComponentManager> managerList;
    private DefineTool.StreamEnum stream;
    private final static Response EMPTY_RESPONSE = new Response();

    /**
     * 初始化
     */
    @Override
    public void init() {
        stream = DefineTool.StreamEnum.OTHER;
        System.out.println("{\"streamInfo\":\"init OtherStream begin\"}");

        managerList = new ArrayList<>();
        managerList.add(MXManager.readCache());
        managerList.add(MXManager.readRecallCache());
        managerList.add(MXManager.prepare());
        managerList.add(MXManager.recall());
        managerList.add(MXManager.writeRecallCache());
        managerList.add(MXManager.filter());
        managerList.add(MXManager.scorer());
        managerList.add(MXManager.ranker());
        managerList.add(MXManager.packer());
        managerList.add(MXManager.ruler());
        managerList.add(MXManager.writeCache());

        logger.info("{\"streamInfo\":\"init OtherStream successfully\"}");
    }

    /**
     * 推荐主流程
     *
     * @throws Exception
     */
    @Override
    @Trace(dispatcher = true)
    public Response recommend(Request request) {
        OtherDataCollection dc = null;
        Request req = request;
        try {
            dc = ThreadLocalManager.getOtherDataCollection();
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
            logger.info(dc.dcLog());
//            RequestFormat.format(dc);
            LogTool.serializedRequest(req.deepCopy());
            return res;
        } catch (Exception e) {
            e.printStackTrace();
            String message = String.format("request->%s, stream catch exception -> %s" ,req.toString(), e.toString());
            LogTool.printErrorLog(logger, DefineTool.ErrorNoEnum.NORMAL_ERROR.getErrorNo(), message, req, null);
            return setRetryFlag(request);
        }
    }

    /**
     * 列表页服务推荐主流程
     *
     * @param dc,rf 请求参数
     * @throws Exception
     */
    @Trace(dispatcher = true)
    private void coreProcess(OtherDataCollection dc) throws Exception {
        for (IStreamComponentManager manager : managerList) {
            Long managerStartTime = System.nanoTime();
            manager.process(dc);
            Long managerEndTime = System.nanoTime();
            dc.appendToTimeRecord(managerEndTime - managerStartTime, manager.getName());
        }
        Long responseStartTime = System.nanoTime();

        dc.generateResponse();
        Long responseEndTime = System.nanoTime();
        dc.appendToTimeRecord(responseEndTime - responseStartTime, this.getName()+"_responseTime");
    }
}