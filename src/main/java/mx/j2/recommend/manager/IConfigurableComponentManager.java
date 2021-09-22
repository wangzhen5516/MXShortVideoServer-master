package mx.j2.recommend.manager;

import mx.j2.recommend.component.configurable.ConfigurableComponentParser;
import mx.j2.recommend.component.configurable.base.BaseComponentConfig;
import mx.j2.recommend.component.configurable.base.IConfigurableComponent;
import mx.j2.recommend.component.configurable.ComponentConfig;
import mx.j2.recommend.data_model.interfaces.IParser;

import java.util.Map;
import java.util.Set;

/**
 * 可配置组件管理器接口，增加可配置操作
 *
 * @param <T> 组件类型
 */
@Deprecated
public interface IConfigurableComponentManager<T> extends IComponentManager<T> {
    /**
     * 初始化可配置组件
     *
     * @param components    可配置组件列表
     * @param outManageMap  各组件管理器维护的组件映射表
     * @param outNewMap     此次生成的组件映射表
     * @param packagePrefix 各组件实现包名
     */
    default void createConfigurableComponent(final Set<String> components,
                                             final Map<String, T> outManageMap,
                                             final Map<String, IConfigurableComponent<BaseComponentConfig>> outNewMap,
                                             final String packagePrefix) throws Exception {
        IParser<String, IConfigurableComponent<BaseComponentConfig>> parser = new ConfigurableComponentParser(packagePrefix);
        IConfigurableComponent<BaseComponentConfig> componentIt;

        for (String componentStrIt : components) {
            // 组件表已经有实例了
            if (outManageMap.containsKey(componentStrIt)) {
                continue;
            }

            if (ComponentConfig.Format.isConfigurableComponent(componentStrIt)) {// 配置组件
                componentIt = parser.parse(componentStrIt);// 生成实例
            } else {// 非配置组件不存在，报错
                throw new ClassNotFoundException(componentStrIt);
            }

            outNewMap.put(componentStrIt, componentIt);
        }
    }
}
