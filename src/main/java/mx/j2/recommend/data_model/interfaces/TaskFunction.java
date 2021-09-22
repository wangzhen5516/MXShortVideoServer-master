package mx.j2.recommend.data_model.interfaces;

/**
 * @author ：zhongrenli
 * @date ：Created in 4:11 下午 2020/11/6
 */
@FunctionalInterface
public interface TaskFunction<T, R> {

    /**
     * function interface
     * @param t
     * @return R
     * @throws Exception
     */
    R apply(T t) throws Exception;
}
