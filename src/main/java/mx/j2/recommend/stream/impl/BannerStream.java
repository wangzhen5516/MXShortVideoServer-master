package mx.j2.recommend.stream.impl;

import com.newrelic.api.agent.Trace;
import mx.j2.recommend.data_model.data_collection.BannerDataCollection;
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
 * Banner 推荐流程
 * @author zhongren.li
 */
@ThreadSafe
public class BannerStream extends BaseStream {
    private static Logger logger = LogManager.getLogger(BannerStream.class);
    private List<IStreamComponentManager> managerList;
    private DefineTool.StreamEnum stream;
    private final static Response EMPTY_RESPONSE = new Response();

    /**
     * 初始化
     */
    @Override
    public void init() {
        stream = DefineTool.StreamEnum.BANNER;

        managerList = new ArrayList<>();
        managerList.add(MXManager.prepare());
        managerList.add(MXManager.recall());
        managerList.add(MXManager.filter());
        managerList.add(MXManager.packer());
        managerList.add(MXManager.ruler());

        System.out.println("{\"streamInfo\":\"init BannerStream begin\"}");
    }

    /**
     * 推荐主流程
     *
     * @throws Exception
     */
    @Override
    @Trace(dispatcher = true)
    public Response recommend(Request request) {
        BannerDataCollection dc = null;
        try {
            dc = ThreadLocalManager.getBannerDataCollection();
            ThreadLocalManager.setDC(dc);
            dc.clean();
            dc.startTime = System.currentTimeMillis();
            if (dc.loadRequest(request)) {
                Long coreProcessStartTime = System.nanoTime();
                coreProcess(dc);
                Long coreProcessEndTime = System.nanoTime();
                dc.appendToTimeRecord(coreProcessEndTime - coreProcessStartTime, "coreProcessTime");
            } else {
                String message = String.format("unknown interfaceName, req -> %s", request.toString());
                LogTool.printErrorLog(logger, DefineTool.ErrorNoEnum.NORMAL_ERROR.getErrorNo(), message, dc.req, null);
                return EMPTY_RESPONSE;
            }
            Response res = dc.data.response;
            dc.endTime = System.currentTimeMillis();
            logger.info(dc.dcLog());
//            RequestFormat.format(dc);
            LogTool.serializedRequest(request.deepCopy());
            return res;
        } catch (Exception e) {
            e.printStackTrace();
            String message = String.format("request->%s, stream catch exception -> %s" , request.toString(), e.toString());
            LogTool.printErrorLog(logger, DefineTool.ErrorNoEnum.NORMAL_ERROR.getErrorNo(), message, request, null);
            return EMPTY_RESPONSE;
        }
    }

    /**
     * 列表页服务推荐主流程
     *
     * @param dc,rf 请求参数
     * @throws Exception
     */
    @Trace(dispatcher = true)
    private void coreProcess(BannerDataCollection dc) throws Exception {
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