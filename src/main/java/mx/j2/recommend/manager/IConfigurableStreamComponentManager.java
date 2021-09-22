package mx.j2.recommend.manager;

import mx.j2.recommend.component.configurable.base.BaseComponentConfig;
import mx.j2.recommend.component.configurable.base.IConfigurableComponent;
import mx.j2.recommend.component.stream.base.IStreamComponent;
import mx.j2.recommend.data_source.ComponentDataSource;
import mx.j2.recommend.util.MXJudgeUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * 可配置流组件管理器接口，增加流操作
 *
 * @param <T> 组件类型
 */
@Deprecated
public interface IConfigurableStreamComponentManager<T> extends IConfigurableComponentManager<T> {
    /**
     * 创建可配置组件入口
     */
    @Deprecated
    void createConfigurableComponent() throws Exception;

    /**
     * 初始化可配置组件
     *
     * @param type   可配置组件类型
     * @param outMap 各组件管理器维护的组件映射表
     */
    @Deprecated
    default void createConfigurableStreamComponent(final IStreamComponent.TypeEnum type,
                                                   final Map<String, T> outMap) throws Exception {
        Set<String> components = ComponentDataSource.INSTANCE.getComponents(type);
        if (MXJudgeUtils.isEmpty(components)) {
            return;
        }

        // 收集可配置组件实例
        Map<String, IConfigurableComponent<BaseComponentConfig>> confMap = new HashMap<>();

        // 初始化可配置组件
        createConfigurableComponent(components, outMap, confMap, type.getInstancePackagePrefix());

        // 转换为流组件存入
        for (Map.Entry<String, IConfigurableComponent<BaseComponentConfig>> entry : confMap.entrySet()) {
            outMap.put(entry.getKey(), (T) entry.getValue());
        }
    }

    /**
     * 创建需要的（可配置）的组件，并检查组件的存在性
     */
    @Deprecated
    void createAndCheckComponents() throws Exception;
}
