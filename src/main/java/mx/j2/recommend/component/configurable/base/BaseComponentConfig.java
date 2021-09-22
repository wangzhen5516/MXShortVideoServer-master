package mx.j2.recommend.component.configurable.base;

import mx.j2.recommend.component.configurable.BasicListConfigParser;
import mx.j2.recommend.component.configurable.ComponentConfig;
import mx.j2.recommend.component.configurable.ConfigValuePair;
import mx.j2.recommend.util.MXStringUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 组件配置基类
 */
public abstract class BaseComponentConfig {
    public static final String KEY_NAME = "name";// 可以给该组件实例起一个名字

    /**
     * 所有的配置
     */
    private Map<String, Object> configs = new HashMap<>();

    BaseComponentConfig(Map<String, ConfigValuePair> confMap) throws Exception {
        // 解析并存储所有的配置信息
        for (Map.Entry<String, ConfigValuePair> entry : confMap.entrySet()) {
            checkNotBlank(entry.getValue().content);
            configs.put(entry.getKey(), parseValue(entry.getValue()));
        }
    }

    public String getName() {
        return getString(KEY_NAME);
    }

    public String getString(String key) {
        return (String) configs.get(key);
    }

    public List<String> getStringList(String key) {
        return (List<String>) configs.get(key);
    }

    public int getInt(String key) {
        return (int) configs.get(key);
    }

    public List<Integer> getIntList(String key) {
        return (List<Integer>) configs.get(key);
    }

    public long getLong(String key) {
        return (long) configs.get(key);
    }

    public List<Long> getLongList(String key) {
        return (List<Long>) configs.get(key);
    }

    public float getFloat(String key) {
        return (float) configs.get(key);
    }

    public List<Float> getFloatList(String key) {
        return (List<Float>) configs.get(key);
    }

    public double getDouble(String key) {
        return (double) configs.get(key);
    }

    public List<Double> getDoubleList(String key) {
        return (List<Double>) configs.get(key);
    }

    public boolean getBoolean(String key) {
        return (boolean) configs.get(key);
    }

    public List<Boolean> getBooleanList(String key) {
        return (List<Boolean>) configs.get(key);
    }

    public Object getObject(String key) {
        return configs.get(key);
    }

    @Override
    public String toString() {
        return configs.toString();
    }

    /**
     * 检查配置的值不能为空白
     */
    private static void checkNotBlank(String value) throws Exception {
        if (MXStringUtils.isBlank(value)) {
            throw new Exception("Invalid value.");
        }
    }

    /**
     * 根据值的类型和字符串表示，解析出一个具体的值
     */
    protected Object parseValue(ConfigValuePair valuePair) throws Exception {
        if (valuePair.type.equals(Byte.class)) {
            return Byte.parseByte(valuePair.content);
        } else if (valuePair.type.equals(String.class)) {
            return valuePair.content;
        } else if (valuePair.type.equals(Integer.class)) {
            return Integer.parseInt(valuePair.content);
        } else if (valuePair.type.equals(Long.class)) {
            return Long.parseLong(valuePair.content);
        } else if (valuePair.type.equals(Float.class)) {
            return Float.parseFloat(valuePair.content);
        } else if (valuePair.type.equals(Double.class)) {
            return Double.parseDouble(valuePair.content);
        } else if (valuePair.type.equals(Boolean.class)) {
            return Boolean.parseBoolean(valuePair.content);
        } else if (ComponentConfig.Items.BasicList.class.isAssignableFrom(valuePair.type)) {// 列表类型
            return new BasicListConfigParser(valuePair.type).parse(valuePair.content);
        } else {
            throw new Exception("Nonsupport config type.");
        }
    }
}
