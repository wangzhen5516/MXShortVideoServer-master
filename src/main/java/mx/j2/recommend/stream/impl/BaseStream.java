package mx.j2.recommend.stream.impl;

import mx.j2.recommend.stream.RecommendStream;
import mx.j2.recommend.thrift.InternalRequest;
import mx.j2.recommend.thrift.InternalResponse;
import mx.j2.recommend.thrift.Request;
import mx.j2.recommend.thrift.Response;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 *  Base 流
 * @author zhongren.li
 */
public abstract class BaseStream implements RecommendStream {
    private static final Logger logger = LogManager.getLogger(BaseStream.class);

    @Override
    public String getName() {
        return this.getClass().getSimpleName();
    }

    public BaseStream() {
        init();
    }

    /**
     * 初始化
     */
    public abstract void init();

    @Override
    public Response recommend(Request request) throws Exception {
        String message = String.format("req: %s -> goes into recommend of BaseStream, please check", request);
        logger.info(message);
        return new Response();
    }

    @Override
    public Response fetchTabs(Request request) {
        String message = String.format("req: %s -> goes into fetchTabs of BaseStream, please check", request);
        logger.info(message);
        return new Response();
    }

    @Override
    public Response fetchBanners(Request request) {
        String message = String.format("req: %s -> goes into fetchBanners of BaseStream, please check", request);
        logger.info(message);
        return new Response();
    }

    @Override
    public InternalResponse internalRecommend(InternalRequest request) {
        String message = String.format("req: %s -> goes into internalRecommend of BaseStream, please check", request);
        logger.info(message);
        return new InternalResponse();
    }

    /**
     * 默认不支持保底
     */
    @Override
    public boolean hasFallback(String interfaceName, boolean force) {
        return false;
    }

    /**
     * 默认保底返回空数据
     */
    @Override
    public Response fallback(Request request) {
        return new Response();
    }

    @Override
    public Response setRetryFlag(Request request) {
        Response res = new Response();
        res.setNeedRetry(true);
        return res;
    }
}
