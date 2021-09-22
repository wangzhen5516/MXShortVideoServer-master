package mx.j2.recommend.prefilter;

/**
 * @author liangjie.feng
 * @version 1.0
 * @date 2021/5/11 下午2:56
 * @description
 */
public interface IPreFilter<T> {
    void preFilter(T dc);
}
