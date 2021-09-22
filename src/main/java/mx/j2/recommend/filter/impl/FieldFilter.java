package mx.j2.recommend.filter.impl;

import mx.j2.recommend.component.configurable.ComponentConfig;
import mx.j2.recommend.component.configurable.config.FilterConfig;
import mx.j2.recommend.component.list.skip.ISkip;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_model.interfaces.IField;
import mx.j2.recommend.util.ClassUtil;

import javax.annotation.Nullable;
import java.lang.reflect.Field;
import java.util.Map;

/**
 * @author zhendong.wang
 * @version 1.0
 * @date 2021/7/16 下午4:18
 * @description 简单字段比较过滤器
 */
@SuppressWarnings("unused")
public class FieldFilter extends BaseFilter<BaseDataCollection> implements IField<BaseDocument> {
    private static final String KEY_FIELD = "field";
    private static final String KEY_OPERATOR = "op";
    private static final String KEY_VALUE = "value";

    @Override
    public void registerConfig(Map<String, Class> outConfMap) {
        outConfMap.put(FilterConfig.KEY_SKIP, ISkip.class);
        outConfMap.put(KEY_FIELD, String.class);
        outConfMap.put(KEY_OPERATOR, String.class);
        outConfMap.put(KEY_VALUE, String.class);
    }

    @Override
    public void setConfig(FilterConfig<BaseDocument, BaseDataCollection> config) throws Exception {
        // 在这里做字段存在性检查
        String field = config.getString(KEY_FIELD);
        if (!exists(BaseDocument.class, field)) {
            throw new NoSuchFieldException();
        }

        // 无异常，继续
        super.setConfig(config);
    }

    @Override
    protected boolean isFiltered(BaseDocument doc, BaseDataCollection dc) {
        Comparable sourceValue = getSourceValue(doc);
        if (sourceValue == null) {
            return false;
        }

        Comparable targetValue = getTargetValue(sourceValue.getClass());
        if (targetValue == null) {
            return false;
        }

        ComponentConfig.Value.BasicOperatorEnum operator = getOperator();
        if (operator == null) {
            return false;
        }

        return operator.test(sourceValue, targetValue);
    }

    @Nullable
    private Comparable getSourceValue(BaseDocument doc) {
        String fieldName = getFieldConfig();

        try {
            Field field = BaseDocument.class.getDeclaredField(fieldName);
            field.setAccessible(true);
            Object sourceValue = field.get(doc);
            return (Comparable) sourceValue;
        } catch (NoSuchFieldException | IllegalAccessException e) {
            e.printStackTrace();
        }

        return null;
    }

    @Nullable
    private Comparable getTargetValue(Class<?> clazz) {
        String value = getValueConfig();
        Object targetValue = ClassUtil.parseValue(value, clazz);
        return (Comparable) targetValue;
    }

    private String getFieldConfig() {
        return config.getString(KEY_FIELD);
    }

    private String getValueConfig() {
        return config.getString(KEY_VALUE);
    }

    @Nullable
    private ComponentConfig.Value.BasicOperatorEnum getOperator() {
        try {
            String operator = getOperatorConfig();
            return ComponentConfig.Value.BasicOperatorEnum.valueOf(operator.toUpperCase());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private String getOperatorConfig() {
        return config.getString(KEY_OPERATOR);
    }
}
