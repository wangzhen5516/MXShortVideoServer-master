package mx.j2.recommend.manager;

/**
 * 组件管理器接口，增加组件操作
 *
 * @param <T> 组件类型
 */
public interface IComponentManager<T> extends IManager {
    /**
     * 数据源准备完毕通知回调
     * 那些依赖数据源的组件管理器，可显式调用和重写此方法
     */
    default void onDataSourcePrepared() {
    }

    /**
     * 获取组件实例
     */
    T getComponentInstance(String name);

    /**
     * 组件接口路径
     */
    String getComponentInterfacePath();
}
