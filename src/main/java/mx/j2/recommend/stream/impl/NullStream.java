package mx.j2.recommend.stream.impl;

import com.newrelic.api.agent.Trace;
import mx.j2.recommend.thrift.Request;
import mx.j2.recommend.thrift.Response;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.LogTool;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.concurrent.ThreadSafe;


/**
 * 推荐主流程
 *
 * @author zhuowei
 */
@ThreadSafe
public class NullStream extends BaseStream {
    private static Logger logger = LogManager.getLogger(NullStream.class);
    private DefineTool.StreamEnum stream;
    private final static Response EMPTY_RESPONSE = new Response();

    public NullStream() {
    }

    /**
     * 初始化
     */
    @Override
    public void init() {
        stream = DefineTool.StreamEnum.NULL;
        System.out.println("{\"streamInfo\":\"init NullStream begin\"}");
        logger.info("{\"streamInfo\":\"init NullStream successfully\"}");
    }

    /**
     * 推荐接口, 推荐卡片
     */
    @Override
    @Trace(dispatcher = true)
    public Response recommend(Request request) {
        String message = String.format("req: %s -> goes into nullStream, please check", request);
        LogTool.printErrorLog(logger, DefineTool.ErrorNoEnum.NORMAL_ERROR.getErrorNo(), message, request, null);
        return EMPTY_RESPONSE;
    }

    @Override
    public Response setRetryFlag(Request request) {
        return new Response();
    }

}