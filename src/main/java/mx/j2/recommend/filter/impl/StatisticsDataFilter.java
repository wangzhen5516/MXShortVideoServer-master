package mx.j2.recommend.filter.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_model.document.StatisticsDocument;
import mx.j2.recommend.data_source.FilterFieldDataSource;
import mx.j2.recommend.manager.DataSourceManager;
import mx.j2.recommend.util.OptionalUtil;
import mx.j2.recommend.util.annotation.FilterField;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author zhongrenli
 */
public class StatisticsDataFilter extends BaseFilter<BaseDataCollection> {

    private final static String VIEW_STR = "viewAll30d";
    private final static String RANGE_HIGH = "poolLevelHigh";
    private final static String RANGE_LOW = "poolLevelLow";

    private final static Set<String> SET = new HashSet<String>(){
        {
            add(VIEW_STR);add(RANGE_HIGH);add(RANGE_LOW);
        }
    };

    @Override
    public boolean skip(BaseDataCollection dc) {
        FilterFieldDataSource dataSource = DataSourceManager.INSTANCE.getFilterFieldDataSource();
        Map<String, Double> map = dataSource.getFieldThresholdMap(dc.recommendFlow.name);
        if (null == map || map.isEmpty()) {
            return true;
        }

        return Double.compare(map.get(VIEW_STR), -1) == 0;
    }

    @Override
    public boolean isFilted(BaseDocument doc, BaseDataCollection dc) {
        StatisticsDocument statistics = doc.statisticsDocument;
        if (!OptionalUtil.ofNullable(statistics).isPresent()) {
            return false;
        }

        FilterFieldDataSource dataSource = DataSourceManager.INSTANCE.getFilterFieldDataSource();
        Map<String, Double> map = dataSource.getFieldThresholdMap(dc.recommendFlow.name);
        assert map.containsKey(VIEW_STR);
        assert map.containsKey(RANGE_LOW);
        assert map.containsKey(RANGE_HIGH);

        if (doc.getPriority() <= map.get(RANGE_LOW)) {
            return false;
        }
        if (Double.compare(map.get(RANGE_HIGH), -1) != 0) {
            if (doc.getPriority() > map.get(RANGE_HIGH)) {
                return false;
            }
        }

        try {
            Field view = doc.statisticsDocument.getClass().getDeclaredField(VIEW_STR);
            if (view.getDouble(doc.statisticsDocument) < map.get(VIEW_STR)) {
                return false;
            }
        } catch (NoSuchFieldException | IllegalAccessException e) {
            e.printStackTrace();
            return false;
        }


        for (Map.Entry<String, Double> entry : map.entrySet()) {
            if (SET.contains(entry.getKey())) {
                continue;
            }
            try {
                Field field = doc.statisticsDocument.getClass().getDeclaredField(entry.getKey());
                if (!OptionalUtil.ofNullable(field).isPresent()
                        || !field.isAnnotationPresent(FilterField.class)) {
                    continue;
                }
                double score = field.getDouble(doc.statisticsDocument);
                if (score < entry.getValue()) {
                    return true;
                }
            } catch (NoSuchFieldException | IllegalAccessException e) {
                e.printStackTrace();
            }
        }

        return false;
    }
}
