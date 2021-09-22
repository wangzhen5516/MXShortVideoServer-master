package mx.j2.recommend.component.configurable;

import mx.j2.recommend.manager.IComponentManager;

import java.util.List;

/**
 * 单个对象配置解析器
 */
public final class ObjectConfigParser extends ListConfigParser {

    public ObjectConfigParser(IComponentManager componentManager) {
        super(componentManager);
    }

    public Object parseObject(String configStr) throws Exception {
        // 先转成列表格式配置
        configStr = ComponentConfig.Format.toList(configStr);

        // 利用父类方法解析列表
        List<Object> objects = super.parse(configStr);

        // 返回第一个元素
        return objects.get(0);
    }
}
