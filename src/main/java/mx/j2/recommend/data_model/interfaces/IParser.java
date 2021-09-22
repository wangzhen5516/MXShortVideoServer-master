package mx.j2.recommend.data_model.interfaces;

/**
 * 解析器接口
 * @param <I> 输入参数类型
 * @param <R> 返回值类型
 */
public interface IParser<I, R> {
    R parse(I inParam) throws Exception;
}
