package mx.j2.recommend.component.configurable;

import mx.j2.recommend.util.MXStringUtils;

/**
 * @author zhendong.wang
 * @version 1.0
 * @date 2021/7/3 上午10:35
 * @description 组件配置定义
 */
public interface ComponentConfig {
    /**
     * 格式相关
     */
    interface Format {
        /**
         * 配置字符串的起止字符
         */
        String BEGIN = "\\{";
        String END = "}";

        /**
         * 键值对之间的分隔符
         */
        String KV_PAIR_SEPARATOR = ",";

        /**
         * 键值对内部的分隔符
         */
        String KV_SEPARATOR = ":";

        /**
         * 数组值之间的分隔符，如[a|b|c]
         */
        String ARRAY_CONTENT_SEPARATOR = "\\|";

        /**
         * 数组起始字符
         */
        String ARRAY_BEGIN = "[";
        String ARRAY_END = "]";

        /**
         * 一个配置值的内部分割符
         */
        String VALUE_INTERNAL_SEPARATOR = "-";

        /**
         * 判断是不是可配置的组件表示
         */
        static boolean isConfigurableComponent(String configStr) {
            if (MXStringUtils.isEmpty(configStr)) {
                return false;
            }
            return configStr.contains(END);
        }

        /**
         * 转成列表配置项
         */
        static String toList(String configStr) {
            return Format.ARRAY_BEGIN + configStr + Format.ARRAY_END;
        }
    }

    /**
     * 解析相关
     */
    interface Parser {
        /**
         * 分割出召回器字段和配置字段
         */
        static String[] split(String original) {
            String[] splits = original.split(Format.BEGIN);

            // 确实有配置信息
            if (splits.length == 2) {
                // 整个配置部分 + "}"
                String conf = splits[1];

                // 结束符位置
                int endIndex = conf.lastIndexOf(Format.END);

                // 找到了结束符，把它去掉
                if (endIndex >= 0) {
                    conf = conf.substring(0, endIndex);
                    splits[1] = conf;
                }
            }

            return splits;
        }

    }

    /**
     * 配置项
     */
    interface Items {
        interface BasicList {

        }

        interface IntegerList extends BasicList {

        }

        interface LongList extends BasicList {

        }

        interface FloatList extends BasicList {

        }

        interface DoubleList extends BasicList {

        }

        interface BooleanList extends BasicList {

        }

        interface StringList extends BasicList {

        }
    }

    /**
     * 配置值相关
     */
    interface Value {
        /**
         * 基础数据类型比较运算符
         */
        enum BasicOperatorEnum {
            EQ("eq") {
                @Override
                public boolean test(Comparable o1, Comparable o2) {
                    return o1.compareTo(o2) == 0;
                }
            },
            NEQ("neq") {
                @Override
                public boolean test(Comparable o1, Comparable o2) {
                    return o1.compareTo(o2) != 0;
                }
            },
            LT("lt") {
                @Override
                public boolean test(Comparable o1, Comparable o2) {
                    return o1.compareTo(o2) < 0;
                }
            },
            GT("gt") {
                @Override
                public boolean test(Comparable o1, Comparable o2) {
                    return o1.compareTo(o2) > 0;
                }
            },
            LTE("lte") {
                @Override
                public boolean test(Comparable o1, Comparable o2) {
                    return o1.compareTo(o2) <= 0;
                }
            },
            GTE("gte") {
                @Override
                public boolean test(Comparable o1, Comparable o2) {
                    return o1.compareTo(o2) >= 0;
                }
            };

            public final String configValue;

            BasicOperatorEnum(String configValue) {
                this.configValue = configValue;
            }

            public abstract boolean test(Comparable o1, Comparable o2);
        }
    }
}
