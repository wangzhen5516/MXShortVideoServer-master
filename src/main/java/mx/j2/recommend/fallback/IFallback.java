package mx.j2.recommend.fallback;

/**
 * 数据保底处理接口
 */
public interface IFallback<T> {
    void fallback(T dc);
}
