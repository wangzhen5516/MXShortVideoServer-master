package mx.j2.recommend.prepare;

/**
 * @author zhendong.wang
 * @version 1.0
 * @date 2021/7/9 下午2:08
 * @description 准备组件接口
 */
public interface IPrepare<T> {
    void run(T dc);
}
