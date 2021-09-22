package mx.j2.recommend.component.configurable.config;

import mx.j2.recommend.component.configurable.ConfigValuePair;
import mx.j2.recommend.component.configurable.ListConfigParser;
import mx.j2.recommend.component.configurable.ObjectConfigParser;
import mx.j2.recommend.component.configurable.base.BaseStreamComponentConfig;
import mx.j2.recommend.component.list.check.ICheck;
import mx.j2.recommend.component.list.check.ICheckList;
import mx.j2.recommend.manager.MXManager;

import java.util.List;
import java.util.Map;

/**
 * 过滤器配置类
 *
 * @param <T> 测试数据类型
 * @param <D> 辅助数据类型
 */
public final class FilterConfig<T, D> extends BaseStreamComponentConfig<D> {
    public static final String KEY_PASS = "pass";// 放行项（不过滤）
    public static final String KEY_TEST = "test";// 检测项

    public FilterConfig(Map<String, ConfigValuePair> confMap) throws Exception {
        super(confMap);
    }

    /**
     * 放行列表
     */
    public List<ICheck<T, D>> getPassList() {
        return (List<ICheck<T, D>>) getObject(KEY_PASS);
    }

    /**
     * 检测项
     */
    public ICheck<T, D> getTest() {
        return (ICheck<T, D>) getObject(KEY_TEST);
    }

    /**
     * 解析放行列表和检测项
     */
    @Override
    protected Object parseValue(ConfigValuePair valuePair) throws Exception {
        if (valuePair.type.equals(ICheckList.class)) {// 放行列表
            return new ListConfigParser(MXManager.check()).parse(valuePair.content);
        } else if (valuePair.type.equals(ICheck.class)) {// 检测项
            return new ObjectConfigParser(MXManager.check()).parseObject(valuePair.content);
        }

        return super.parseValue(valuePair);
    }
}
