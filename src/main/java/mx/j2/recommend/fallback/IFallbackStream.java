package mx.j2.recommend.fallback;

import mx.j2.recommend.thrift.Request;
import mx.j2.recommend.thrift.Response;

/**
 * 数据流保底处理接口
 */
public interface IFallbackStream {
    /**
     * 当前是否有保底流程可以走，需要满足如下条件:
     * 1 该流必须支持保底（目前 feed 流支持保底）
     * 2 保底开关必须是打开的
     *
     * @param force true 返回是否强制保底，false 返回保底是否开启
     */
    boolean hasFallback(String interfaceName, boolean force);

    /**
     * 保底流程
     */
    Response fallback(Request request);

    Response setRetryFlag(Request request);
}
