package mx.j2.recommend.component.configurable.config;

import mx.j2.recommend.component.configurable.ConfigValuePair;
import mx.j2.recommend.component.configurable.base.BaseStreamComponentConfig;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.util.MXJudgeUtils;

import java.util.Map;

/**
 * 混入器配置类
 *
 * @param <T> DC 类型
 * @see BaseDataCollection
 */
public final class MixerConfig<T> extends BaseStreamComponentConfig<T>
        implements BaseDataCollection.IResult {
    public static final String KEY_COUNT = "count";// 混入数量
    public static final String KEY_TYPE = "type";// 混入方式
    public static final String KEY_TABLE = "table";// 数据库表名

    /**
     * 混入方式
     */
    public enum TypeEnum {
        RANDOM("random"),
        IN_ORDER("in_order");

        // 配置这个值即可
        public final String type;

        TypeEnum(String type) {
            this.type = type;
        }
    }

    public MixerConfig(Map<String, ConfigValuePair> confMap) throws Exception {
        super(confMap);
    }

    /**
     * 混入数量
     */
    public float getCount() {
        return getFloat(KEY_COUNT);
    }

    /**
     * 混入方式
     */
    public TypeEnum getType() {
        String type = getString(KEY_TYPE);

        if (MXJudgeUtils.isEmpty(type)) {
            return TypeEnum.IN_ORDER;
        }

        try {
            return TypeEnum.valueOf(type.toUpperCase());
        } catch (Exception e) {
            e.printStackTrace();
        }

        return TypeEnum.IN_ORDER;
    }

    /**
     * 数据库表
     */
    public String getTable() {
        return getString(KEY_TABLE);
    }

    /**
     * 数据访问键
     */
    @Override
    public String getResultKey() {
        return getString(KEY_RESULT);
    }
}
