package mx.j2.recommend.util;

import com.alibaba.fastjson.JSONObject;

import javax.annotation.Nullable;
import java.util.List;

public class MXJsonUtils {

    public static String getStringValue(JSONObject source, String key) {
        return getValue(source, key, String.class, "");
    }

    public static Boolean getBooleanValue(JSONObject source, String key) {
        return getValue(source, key, Boolean.class, false);
    }

    public static long getLongValue(JSONObject source, String key) {
        return getValue(source, key, Long.class, 0L);
    }

    public static int getIntValue(JSONObject source, String key) {
        return getValue(source, key, Integer.class, 0);
    }

    public static float getFloatValue(JSONObject source, String key) {
        return getValue(source, key, Float.class, 0F);
    }

    public static double getDoubleValue(JSONObject source, String key) {
        return getValue(source, key, Double.class, 0D);
    }

    /**
     * 获取字段值的通用模板方法
     */
    private static <T> T getValue(JSONObject source, String key, Class<T> clazz, T defaultValue) {
        if (source.containsKey(key)) {
            return source.getObject(key, clazz);
        }
        return defaultValue;
    }

    /**
     * 获取字段值的通用模板方法
     */
    @Nullable
    public static Object getValue(JSONObject source, String key, Class clazz) {
        if (source.containsKey(key)) {
            return source.getObject(key, clazz);
        }
        return null;
    }

    public static List<String> getStringListValue(JSONObject source, String key) {
        return getListValue(source, key, String.class);
    }

    /**
     * 获取列表字段值的通用模板方法
     */
    @Nullable
    private static <T> List<T> getListValue(JSONObject source, String key, Class<T> clazz) {
        if (source.containsKey(key)) {
            return source.getJSONArray(key).toJavaList(clazz);
        }
        return null;
    }
}
