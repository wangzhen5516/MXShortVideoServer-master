package mx.j2.recommend.component.configurable.base;

import mx.j2.recommend.component.configurable.ConfigValuePair;
import mx.j2.recommend.component.configurable.config.MixerConfig;
import mx.j2.recommend.component.list.skip.ISkip;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.mixer.IMixer;

import java.util.Map;

/**
 * @author : zhendong.wang
 * @date : 2021/2/26
 * @description 可配置混入器基类
 *
 * @param <T> DC 类型
 */
public abstract class BaseConfigurableMixer<T extends BaseDataCollection>
        extends BaseConfigurableStreamComponent<T, MixerConfig<T>>
        implements IMixer<T>, BaseDataCollection.IResult {

    @Override
    public void registerConfig(Map<String, Class> outConfMap) {
        outConfMap.put(MixerConfig.KEY_SKIP, ISkip.class);
        outConfMap.put(MixerConfig.KEY_COUNT, Float.class);
        outConfMap.put(MixerConfig.KEY_TYPE, String.class);
        outConfMap.put(MixerConfig.KEY_RESULT, String.class);
    }

    @Override
    public MixerConfig<T> newConfig(Map<String, ConfigValuePair> confMap) throws Exception {
        return new MixerConfig<>(confMap);
    }

    @Override
    public String getResultKey() {
        return config != null ? config.getResultKey() : "";
    }

    /**
     * 混入数量
     */
    public float getCount(T dc) {
        return config != null ? config.getCount() : 0;
    }

    /**
     * 混入方式
     */
    public MixerConfig.TypeEnum getType() {
        return config != null ? config.getType() : MixerConfig.TypeEnum.IN_ORDER;
    }
}