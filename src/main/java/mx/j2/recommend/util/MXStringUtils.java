package mx.j2.recommend.util;

import org.apache.commons.lang.StringUtils;

public class MXStringUtils {

    public static boolean isEmpty(CharSequence str) {
        return str == null || str.length() == 0;
    }

    public static boolean isNotEmpty(String str) {
        return !isEmpty(str);
    }

    public static boolean isBlank(String str) {
        return StringUtils.isBlank(str);
    }

    public static boolean isNotBlank(String str) {
        return !isBlank(str);
    }

    public static String strip(String str) {
        return StringUtils.strip(str);
    }

    public static boolean isNumeric(String str) {
        return StringUtils.isNumeric(str);
    }

    public static String[] split(String str, String separatorChars) {
        return StringUtils.split(str, separatorChars);
    }

    /**
     * 默认后缀
     */
    public static String toSuffix(String key) {
        return "_" + key;
    }

    /**
     * 格式后缀
     */
    public static String toSuffix(String format, String key) {
        return String.format(format, key);
    }
}
