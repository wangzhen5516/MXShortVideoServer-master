package mx.j2.recommend.component.list.check;

/**
 * 检查接口
 *
 * @param <T> 检测数据类型
 * @param <U> 辅助数据类型
 */
public interface ICheck<T, U> {
    /**
     * 逻辑检查
     */
    boolean check(T testData, U utilData);
}
