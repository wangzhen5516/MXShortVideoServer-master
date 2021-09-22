package mx.j2.recommend.component.configurable;

import mx.j2.recommend.component.configurable.base.BaseComponentConfig;
import mx.j2.recommend.component.configurable.base.IConfigurableComponent;
import mx.j2.recommend.data_model.interfaces.IParser;

import java.util.HashMap;
import java.util.Map;

/**
 * 可配置组件解析器
 */
public final class ComponentConfigParser implements IParser<String, BaseComponentConfig> {
    // 可配置组件
    private IConfigurableComponent<BaseComponentConfig> component;

    ComponentConfigParser(IConfigurableComponent<BaseComponentConfig> component) {
        this.component = component;
    }

    @Override
    public BaseComponentConfig parse(String confStr) throws Exception {
        // 解析生成配置 KV 映射
        Map<String, String> kvMap = new ConfigMapParser().parse(confStr);

        // 各组件注册自己的配置项
        Map<String, Class> keyMap = new HashMap<>();
        component.registerConfig(keyMap);

        // 检查配置项和注册项是否一致
        if (!keyMap.keySet().equals(kvMap.keySet())) {
            throw new Exception("Component config is not matched with it's registering: " + component.getFullName());
        }

        // 生成 Key,Value-Type 映射
        Map<String, ConfigValuePair> confMap = new HashMap<>();
        for (Map.Entry<String, Class> entry : keyMap.entrySet()) {
            confMap.put(entry.getKey(), new ConfigValuePair(kvMap.get(entry.getKey()), entry.getValue()));
        }

        // 构造配置实体
        return component.newConfig(confMap);
    }
}
