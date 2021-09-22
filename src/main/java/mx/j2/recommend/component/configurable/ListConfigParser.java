package mx.j2.recommend.component.configurable;

import mx.j2.recommend.data_model.interfaces.IParser;
import mx.j2.recommend.manager.IComponentManager;

import java.util.ArrayList;
import java.util.List;

/**
 * 列表配置解析器
 * <p>
 * 例：
 * [classname1|classname2]
 */
public class ListConfigParser implements IParser<String, List<Object>> {
    // 组件实例管理器
    private IComponentManager componentManager;

    public ListConfigParser(IComponentManager componentManager) {
        this.componentManager = componentManager;
    }

    @Override
    public List<Object> parse(String configStr) throws Exception {
        // 检查配置合法性
        if (!configStr.startsWith(ComponentConfig.Format.ARRAY_BEGIN)
                || !configStr.endsWith(ComponentConfig.Format.ARRAY_END)) {
            throw new Exception("List config must wrap with [].");
        }

        // 掐头去尾去掉 []
        configStr = configStr.substring(1, configStr.length() - 1);

        // 切分所有的名字
        String[] names = configStr.split(ComponentConfig.Format.ARRAY_CONTENT_SEPARATOR);

        // 去实例池子（管理器）拿实例填充列表
        Object componentIt;
        List<Object> components = new ArrayList<>();
        for (String nameIt : names) {
            componentIt = getObject(nameIt);

            if (componentIt == null) {
                throw new Exception("Class " + nameIt + " not found.");
            }

            components.add(componentIt);
        }

        return components;
    }

    Object getObject(String objName) {
        return componentManager.getComponentInstance(objName);
    }
}
