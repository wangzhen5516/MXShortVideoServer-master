package mx.j2.recommend.stream;

import mx.j2.recommend.fallback.IFallbackStream;
import mx.j2.recommend.thrift.InternalRequest;
import mx.j2.recommend.thrift.InternalResponse;
import mx.j2.recommend.thrift.Request;
import mx.j2.recommend.thrift.Response;

/**
 *
 * @author zhongren.li
 * @date 2018/1/10
 */
public interface RecommendStream extends IFallbackStream {
    /**
     * 获取RecommendStream的名字
     * @return name
     */
    String getName();

    /**
     * 推荐流程
     * @param request request
     * @return response
     */
    Response recommend(Request request) throws Exception;

    /**
     * 拉取卡片列表
     * @param request request
     * @return response
     */
    Response fetchTabs(Request request);

    /**
     * 拉取banner 列表
     * @param request request
     * @return response
     */
    Response fetchBanners(Request request);

    /**
     * 内部推荐流程
     * @param request request
     * @return internal response
     */
    InternalResponse internalRecommend(InternalRequest request);
}
