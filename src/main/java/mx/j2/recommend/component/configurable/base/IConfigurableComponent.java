package mx.j2.recommend.component.configurable.base;

import mx.j2.recommend.component.base.IComponent;
import mx.j2.recommend.component.configurable.ConfigValuePair;

import java.util.Map;

/**
 * 可配置组件接口，任何想要实现可配置的组件均实现此接口
 *
 * @param <C> 配置类型
 * @see BaseComponentConfig
 */
public interface IConfigurableComponent<C> extends IComponent, IConfigurable {
    /**
     * 注册配置项，所有需要的配置项都要添加上，而且一旦注册了就必须要有配置
     *
     * @param outConfMap <键，值类型>
     */
    void registerConfig(Map<String, Class> outConfMap);

    /**
     * 构造配置实体，各组件实例提供自己的配置实例
     *
     * @param confMap <键, 值-类型>
     */
    C newConfig(Map<String, ConfigValuePair> confMap) throws Exception;

    /**
     * 应用配置实体
     */
    void setConfig(C conf) throws Exception;

    /**
     * 获取当前配置
     */
    C getConfig();

    /**
     * 组件全称（包括配置串），作为唯一标识
     */
    void setFullName(String fullName);

    String getFullName();

    /**
     * 组件唯一标识，因为是可配置的，所以类名已经不够用了
     */
    String getId();
}
