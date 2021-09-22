package mx.j2.recommend.ruler.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;

import java.util.List;
import java.util.Map;

/**
 * "少即是无"规则
 */
@SuppressWarnings("unused")
public class LessIsNoneRuler<T extends BaseDataCollection> extends BaseRuler<T> {
    private static final String KEY_MINIMUM = "min";

    @Override
    public void rule(T dc) {
        List list = dc.getResultList();

        if (list.size() < config.getInt(KEY_MINIMUM)) {
            list.clear();
        }
    }

    @Override
    public void registerConfig(Map<String, Class> outConfMap) {
        super.registerConfig(outConfMap);
        outConfMap.put(KEY_MINIMUM, Integer.class);
    }
}
