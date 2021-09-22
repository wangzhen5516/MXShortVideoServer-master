package mx.j2.recommend.component.configurable.base;

import mx.j2.recommend.component.list.match.IMatch;
import mx.j2.recommend.component.list.skip.ISkip;
import mx.j2.recommend.component.stream.base.IStreamComponent;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.util.MXJudgeUtils;

import java.util.List;

/**
 * @param <T> DC 类型
 * @param <C> 配置类型
 * @author zhendong.wang
 * @version 1.0
 * @date 2021/2/26 下午3:54
 * @description 可配置流组件基类
 * @see BaseDataCollection
 * @see BaseComponentConfig
 */
public abstract class BaseConfigurableStreamComponent<T extends BaseDataCollection, C extends BaseStreamComponentConfig<T>>
        extends BaseConfigurableComponent<C> implements IStreamComponent<T> {

    /**
     * 默认执行配置列表
     */
    @Override
    public boolean skip(T baseDC) {
        // 当子类实际上没有配置时，会发生这种情况
        if (config == null) {
            return false;
        }

        List<ISkip<T>> skips = getSkips();
        if (MXJudgeUtils.isEmpty(skips)) {
            return false;
        }

        for (ISkip<T> skipIt : skips) {
            if (skipIt.skip(baseDC)) {
                return true;
            }
        }

        return false;
    }

    private List<ISkip<T>> getSkips() {
        return config.getSkips();
    }

    protected IMatch getMatch() {
        return config.getMatch();
    }

    @Override
    public String getId() {
        return getName();
    }

    @Override
    public boolean prepare(T dc) {
        return true;
    }

    /**
     * 获取准备结果
     */
    protected Object getPrepareResult(T dc, String key) {
        return dc.getPrepareResult(key);
    }
}
