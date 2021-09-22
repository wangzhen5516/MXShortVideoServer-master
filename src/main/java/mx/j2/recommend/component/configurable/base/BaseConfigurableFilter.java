package mx.j2.recommend.component.configurable.base;

import mx.j2.recommend.component.configurable.ConfigValuePair;
import mx.j2.recommend.component.configurable.config.FilterConfig;
import mx.j2.recommend.component.list.check.ICheck;
import mx.j2.recommend.component.list.check.ICheckList;
import mx.j2.recommend.component.list.skip.ISkip;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.filter.IFilter;
import mx.j2.recommend.util.MXJudgeUtils;

import java.util.List;
import java.util.Map;

/**
 * @author : zhendong.wang
 * @date : 2021/2/26
 * @description 可配置过滤器基类
 *
 * @param <T> 测试数据类型
 * @param <D> DC 类型
 */
public abstract class BaseConfigurableFilter<T, D extends BaseDataCollection>
        extends BaseConfigurableStreamComponent<D, FilterConfig<T, D>>
        implements IFilter<D> {

    /**
     * 默认配置
     */
    @Override
    public void registerConfig(Map<String, Class> outConfMap) {
        outConfMap.put(FilterConfig.KEY_SKIP, ISkip.class);
        outConfMap.put(FilterConfig.KEY_PASS, ICheckList.class);
        outConfMap.put(FilterConfig.KEY_TEST, ICheck.class);
    }

    @Override
    public FilterConfig<T, D> newConfig(Map<String, ConfigValuePair> confMap) throws Exception {
        return new FilterConfig<>(confMap);
    }

    /**
     * 判断一个元素是否要被过滤掉
     */
    protected boolean isFiltered(T doc, D dc) {
        // 当子类实际上没有配置时，会发生这种情况
        if (config == null) {
            return false;
        }

        // 先判断是否放行（不过滤）
        List<ICheck<T, D>> passList = getPassList();
        if (MXJudgeUtils.isNotEmpty(passList)) {
            for (ICheck<T, D> passIt : passList) {
                if (passIt.check(doc, dc)) {
                    // 有放行项生效，返回不过滤
                    return false;
                }
            }
        }

        // 不放行，例行检查
        return getTest().check(doc, dc);
    }

    /**
     * 检测项
     */
    private ICheck<T, D> getTest() {
        return config.getTest();
    }

    /**
     * 放行项
     */
    private List<ICheck<T, D>> getPassList() {
        return config.getPassList();
    }
}