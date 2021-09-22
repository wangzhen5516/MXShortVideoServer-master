package mx.j2.recommend.mixer;

/**
 * @author ：zhongrenli
 * @date ：Created in 3:41 下午 2020/8/13
 */
public interface IMixer<T> {
    /**
     * 执行混合
     */
    void mix(T dc);
}
