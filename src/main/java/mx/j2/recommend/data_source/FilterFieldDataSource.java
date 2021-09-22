package mx.j2.recommend.data_source;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import mx.j2.recommend.data_model.document.StatisticsDocument;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.MXCollectionUtils;
import mx.j2.recommend.util.annotation.FilterField;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author ：zhongrenli
 * @date ：Created in 4:41 下午 2021/3/2
 */
public class FilterFieldDataSource extends BaseDataSource{
    private static Map<String, Map<String, Double>> fieldMap;

    private static Map<String, Map<String, Double>> fieldMap2;

    private static final Map<String, Double> DEFAULT_MAP = new HashMap<String, Double>(){
        {
            put("viewAll30d", 1000D);put("downloadRate30d", 0.0003);put("likeRate30d", 0.01);
            put("poolLevelHigh", -1D);put("poolLevelLow", 7D);put("shareRate30d", 0.0006);
        }
    };

    public FilterFieldDataSource() {
        init();
    }

    private void init() {
        try {
            make();
        } catch (NoSuchFieldException | IllegalAccessException e) {
            e.printStackTrace();
        }
        fieldMap = new ConcurrentHashMap<>(16);
        fieldMap2 = new ConcurrentHashMap<>(16);
        ScheduledExecutorService scheduledExecutorService = new ScheduledThreadPoolExecutor(1,
                new ThreadFactoryBuilder().setNameFormat("filter-field-%s").build());
        scheduledExecutorService.scheduleAtFixedRate(this::refresh, 1, 1, TimeUnit.MINUTES);
    }

    private void refresh() {
        fillFieldMap(fieldMap, "field_conf");
        fillFieldMap(fieldMap2, "field_conf2");
    }

    private void fillFieldMap(Map<String, Map<String, Double>> fieldMap, String fieldConf) {
        ElasticCacheSource dataSource = MXDataSource.redis();
        Map<String, Map<String, Double>> map = dataSource.getFieldConf(fieldConf);
        if (null == map || map.isEmpty()) {
            return;
        }
        fieldMap.putAll(map);
        List<String> remove = fieldMap.keySet().stream().filter(k -> !map.containsKey(k)).collect(Collectors.toList());
        if (!MXCollectionUtils.isEmpty(remove)) {
            remove.forEach(fieldMap::remove);
        }
        if (!fieldMap.containsKey("base") || fieldMap.get("base").isEmpty()) {
            fieldMap.put("base", DEFAULT_MAP);
        }
    }

    private void make() throws NoSuchFieldException, IllegalAccessException {
        Field[] fields = StatisticsDocument.class.getDeclaredFields();
        for (Field field : fields) {
            if (field.isAnnotationPresent(FilterField.class)) {
                FilterField annotation = field.getAnnotation(FilterField.class);
                InvocationHandler h = Proxy.getInvocationHandler(annotation);
                Field hField = h.getClass().getDeclaredField("memberValues");
                hField.setAccessible(true);
                Map memberValues = (Map)hField.get(h);
                memberValues.put("name", field.getName());
            }
        }
    }

    public Map<String, Double> getFieldThresholdMap(String name) {
        Map<String, Double> temp = fieldMap.get(name);
        if (null == temp || temp.isEmpty()) {
            return new HashMap<>(fieldMap.getOrDefault("base", DEFAULT_MAP));
        }
        return new HashMap<>(temp);
    }

    public Map<String, Double> getFieldThresholdMap2(String name) {
        Map<String, Double> temp = fieldMap2.get(name);
        if (null == temp || temp.isEmpty()) {
            return new HashMap<>(fieldMap2.getOrDefault("base", DEFAULT_MAP));
        }
        return new HashMap<>(temp);
    }
}
