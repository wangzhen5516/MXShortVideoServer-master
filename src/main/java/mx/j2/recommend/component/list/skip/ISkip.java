package mx.j2.recommend.component.list.skip;

/**
 * 跳过执行通用接口
 * @param <T> 用于判断的数据源
 */
public interface ISkip<T> {
    boolean skip(T data);
}
