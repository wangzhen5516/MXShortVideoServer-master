package mx.j2.recommend.data_source;

import mx.j2.recommend.data_model.statistics_document.BaseStatisticsDocument;
import mx.j2.recommend.statistic_conf.StatisticConf;
import mx.j2.recommend.statistic_conf.StatisticConfParser;
import mx.j2.recommend.util.FileTool;
import mx.j2.recommend.util.annotation.StatisticField;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author ：zhongrenli
 * @date ：Created in 2:46 下午 2021/4/17
 */
public class StatisticDataSource extends BaseDataSource{

    private Map<String, StatisticConf> statisticConfMap;

    private Map<String, String> map;

    public StatisticDataSource() {
        init();
    }

    private void init() {
        statisticConfMap = new LinkedHashMap<>();

        String content = FileTool.readContent("./conf/statics_data.json");
        try {
            StatisticConfParser.parse(content, statisticConfMap);
        } catch (Exception e) {
            e.printStackTrace();
        }
        map = new HashMap<>(16);
        initMap();
    }

    public StatisticConf getStatisticConf(String k) {
        return statisticConfMap.getOrDefault(k, null);
    }

    public Map<String, StatisticConf> getStatisticConfMap() {
        return new LinkedHashMap<>(statisticConfMap);
    }

    private void initMap() {
        Field[] fields = BaseStatisticsDocument.class.getDeclaredFields();
        for (Field field : fields) {
            if (field.isAnnotationPresent(StatisticField.class)) {
                StatisticField annotation = field.getAnnotation(StatisticField.class);
                map.put(annotation.name(), field.getName());
            }
        }
    }

    public String getFieldName(String annotation) {
        return map.getOrDefault(annotation, null);
    }
}
