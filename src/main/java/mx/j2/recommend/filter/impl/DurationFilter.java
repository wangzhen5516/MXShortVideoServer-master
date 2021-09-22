package mx.j2.recommend.filter.impl;

import mx.j2.recommend.component.configurable.config.FilterConfig;
import mx.j2.recommend.component.list.skip.ISkip;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;

import java.util.Map;

@SuppressWarnings("unused")
public class DurationFilter extends BaseFilter<BaseDataCollection> {
    private static final String KEY_OPERATOR = "operator";
    private static final String KEY_DURATION = "duration";
    private static final String CONFIG_VALUE_OPERATOR_LT = "<";
    private static final String CONFIG_VALUE_OPERATOR_GT = ">";
    private static final String CONFIG_VALUE_OPERATOR_LTE = "<=";
    private static final String CONFIG_VALUE_OPERATOR_GTE = ">=";

    @Override
    public void registerConfig(Map<String, Class> outConfMap) {
        outConfMap.put(FilterConfig.KEY_SKIP, ISkip.class);
        outConfMap.put(KEY_OPERATOR, String.class);
        outConfMap.put(KEY_DURATION, Long.class);
    }

    @Override
    public boolean isFilted(BaseDocument doc, BaseDataCollection dc) {
        long duration = getDurationConfig();
        String operator = getOperatorConfig();

        /*外部duration可能没数据*/
        long docDuration = doc.duration == 0 ? doc.innerDuration : doc.duration;

        if (CONFIG_VALUE_OPERATOR_GT.equals(operator)) {
            return docDuration > duration;
        } else if (CONFIG_VALUE_OPERATOR_LT.equals(operator)) {
            return docDuration < duration;
        } else if (CONFIG_VALUE_OPERATOR_GTE.equals(operator)) {
            return docDuration >= duration;
        } else if (CONFIG_VALUE_OPERATOR_LTE.equals(operator)) {
            return docDuration <= duration;
        } else {
            return false;
        }
    }

    private String getOperatorConfig() {
        return config.getString(KEY_OPERATOR);
    }

    private long getDurationConfig() {
        return config.getLong(KEY_DURATION);
    }
}
