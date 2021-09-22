package mx.j2.recommend.component.configurable.base;

import mx.j2.recommend.component.configurable.ConfigValuePair;
import mx.j2.recommend.component.configurable.config.PrepareConfig;
import mx.j2.recommend.component.list.skip.ISkip;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.prepare.IPrepare;

import java.util.Map;

/**
 * @param <T> DC 类型
 * @author : zhendong.wang
 * @date : 2021/2/26
 * @description 可配置准备器基类
 */
public abstract class BaseConfigurablePrepare<T extends BaseDataCollection>
        extends BaseConfigurableStreamComponent<T, PrepareConfig<T>>
        implements IPrepare<T> {

    /**
     * 默认配置
     */
    @Override
    public void registerConfig(Map<String, Class> outConfMap) {
        outConfMap.put(PrepareConfig.KEY_SKIP, ISkip.class);
        outConfMap.put(PrepareConfig.KEY_NAME, String.class);
    }

    @Override
    public PrepareConfig<T> newConfig(Map<String, ConfigValuePair> confMap) throws Exception {
        return new PrepareConfig<>(confMap);
    }
}