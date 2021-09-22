package mx.j2.recommend.component.configurable.base;

import mx.j2.recommend.component.configurable.ConfigValuePair;
import mx.j2.recommend.component.configurable.config.RulerConfig;
import mx.j2.recommend.component.list.skip.ISkip;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.ruler.IRuler;

import java.util.Map;

/**
 * @param <T> DC 类型
 * @author : zhendong.wang
 * @date : 2021/2/26
 * @description 可配置规则器基类
 */
public abstract class BaseConfigurableRuler<T extends BaseDataCollection>
        extends BaseConfigurableStreamComponent<T, RulerConfig<T>>
        implements IRuler<T> {

    /**
     * 默认配置
     */
    @Override
    public void registerConfig(Map<String, Class> outConfMap) {
        outConfMap.put(RulerConfig.KEY_SKIP, ISkip.class);
    }

    @Override
    public RulerConfig<T> newConfig(Map<String, ConfigValuePair> confMap) throws Exception {
        return new RulerConfig<>(confMap);
    }
}