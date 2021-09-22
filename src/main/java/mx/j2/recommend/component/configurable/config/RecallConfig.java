package mx.j2.recommend.component.configurable.config;

import mx.j2.recommend.component.configurable.ConfigValuePair;
import mx.j2.recommend.component.configurable.base.BaseStreamComponentConfig;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;

import java.util.Map;

/**
 * 召回器配置类
 *
 * @param <T> DC 类型
 * @see BaseDataCollection
 */
public final class RecallConfig<T> extends BaseStreamComponentConfig<T>
        implements BaseDataCollection.IResult {
    public static final String KEY_SIZE = "size";
    public static final String KEY_ES_INDEX = "es_index";

    public RecallConfig(Map<String, ConfigValuePair> confMap) throws Exception {
        super(confMap);
    }

    /**
     * 召回数量
     */
    public int getSize() {
        return getInt(KEY_SIZE);
    }

    /**
     * 列表键
     */
    @Override
    public String getResultKey() {
        return getString(KEY_RESULT);
    }

    /**
     * ES 索引
     */
    public String getEsIndex() {
        return getString(KEY_ES_INDEX);
    }
}
