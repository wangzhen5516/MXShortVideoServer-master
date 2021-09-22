package mx.j2.recommend.component.configurable;

/**
 * 基础数据列表类型配置解析器
 */
public final class BasicListConfigParser extends ListConfigParser {
    private Class type;

    public BasicListConfigParser(Class type) {
        super(null);
        this.type = type;
    }

    @Override
    Object getObject(String objName) {
        if (type.equals(ComponentConfig.Items.IntegerList.class)) {
            return Integer.parseInt(objName);
        } else if (type.equals(ComponentConfig.Items.LongList.class)) {
            return Long.parseLong(objName);
        } else if (type.equals(ComponentConfig.Items.FloatList.class)) {
            return Float.parseFloat(objName);
        } else if (type.equals(ComponentConfig.Items.DoubleList.class)) {
            return Double.parseDouble(objName);
        } else if (type.equals(ComponentConfig.Items.BooleanList.class)) {
            return Boolean.parseBoolean(objName);
        } else if (type.equals(ComponentConfig.Items.StringList.class)) {
            return String.valueOf(objName);
        } else {
            return null;
        }
    }
}
