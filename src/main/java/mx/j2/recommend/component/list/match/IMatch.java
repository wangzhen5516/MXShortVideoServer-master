package mx.j2.recommend.component.list.match;

/**
 * @param <T1> 靶类型
 * @param <T2> 子弹类型
 * @author zhendong.wang
 * @version 1.0
 * @date 2021/7/12 下午4:36
 * @description 匹配接口
 */
public interface IMatch<T1, T2> {
    boolean matches(T1 t1, T2 t2);
}
