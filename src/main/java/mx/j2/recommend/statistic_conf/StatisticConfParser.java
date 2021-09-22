package mx.j2.recommend.statistic_conf;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import mx.j2.recommend.util.FileTool;

import java.util.*;

/**
 * @author ：zhongrenli
 * @date ：Created in 3:31 下午 2021/4/17
 */
public class StatisticConfParser {

    public static void parse(String content, Map<String, StatisticConf> statisticConfMap) throws Exception {
        JSONArray jsonArray = JSONArray.parseArray(content);
        Map<String, String> indexMap = new HashMap<>(16);
        initIndexMap(jsonArray, indexMap);

        Map<String, StatisticConf> map = new LinkedHashMap<>();
        for (Object o : jsonArray) {
            JSONObject conf = (JSONObject) o;
            if (!conf.containsKey("statics_name")) {
                throw new Exception();
            }
            String name = conf.getString("statics_name");
            if ("statics_list".equals(name)) {
                continue;
            }

            StatisticConf sc = new StatisticConf();
            sc.setStatisticName(name);
            String[] array = name.split("_");
            sc.setSuffix(array[array.length - 1]);

            if (conf.containsKey("priority")) {
                sc.setPriority(conf.getIntValue("priority"));
            }

            if (conf.containsKey("exclude")) {
                JSONArray ja = conf.getJSONArray("exclude");
                if (null != ja && !ja.isEmpty()) {
                    sc.setExclude(ja.toJavaList(String.class));
                }
            }

            if (conf.containsKey("index_list")) {
                JSONArray ja = conf.getJSONArray("index_list");
                if (null != ja && !ja.isEmpty()) {
                    sc.setIndexList(ja.toJavaList(String.class));
                    List<String> s = new ArrayList<>();
                    for (String index : sc.getIndexList()) {
                        if (indexMap.containsKey(index)) {
                            s.add(String.format("%s_%s", indexMap.get(index), sc.getSuffix()));
                        }
                    }
                    if (!s.isEmpty()) {
                        sc.setIndexStringList(s);
                    }
                }
            }

            if (conf.containsKey("description")) {
                sc.setDescription(conf.getString("description"));
            }

            if (conf.containsKey("pre_condition")) {
                analysisCondition(conf, "pre_condition", sc, indexMap);
            }

            if (conf.containsKey("base_condition")) {
                analysisCondition(conf, "base_condition", sc, indexMap);
            }

            if (conf.containsKey("small_flow")) {
                JSONObject small = conf.getJSONObject("small_flow");
                if (null != small && !small.isEmpty()) {
                    for (Map.Entry<String, Object> entry : small.entrySet()) {
                        JSONObject value = (JSONObject)entry.getValue();
                        if (value.containsKey("pre_condition")) {
                            StatisticConf temp = new StatisticConf();
                            analysisCondition(value, "pre_condition", temp, indexMap);
                            sc.putToSmallFlowConf(entry.getKey(), temp);
                        }

                        if (value.containsKey("base_condition")) {
                            StatisticConf temp = new StatisticConf();
                            analysisCondition(value, "base_condition", temp, indexMap);
                            sc.putToSmallFlowConf(entry.getKey(), temp);
                        }
                    }
                }
            }
            map.put(name, sc);
        }
        List<Map.Entry<String, StatisticConf>> list = new ArrayList<>(map.entrySet());
        list.sort(Comparator.comparingInt(o -> o.getValue().getPriority()));
        for (Map.Entry<String, StatisticConf> entry : list) {
            statisticConfMap.put(entry.getKey(), entry.getValue());
        }
    }

    public static void initIndexMap (JSONArray jsonArray, Map<String, String> indexMap) throws Exception {
        for (Object o : jsonArray) {
            JSONObject conf = (JSONObject) o;
            if (!conf.containsKey("statics_name")) {
                throw new Exception();
            }

            String name = conf.getString("statics_name");
            if (!"statics_list".equals(name)) {
                continue;
            }

            JSONObject object = conf.getJSONObject("index_number");
            for (String k : object.keySet()) {
                indexMap.put(k, object.getString(k));
            }
        }
    }

    public static void analysisCondition(JSONObject conf, String field, StatisticConf sc, Map<String, String> indexMap) throws Exception {
        JSONArray condition = conf.getJSONArray(field);
        if (null == condition || condition.isEmpty()) {
            return;
        }

        for (int i = 0; i < condition.size(); i++) {
            String c = condition.getString(i);
            String[] a = c.split(":");
            if (a.length != 3 || !indexMap.containsKey(a[0])) {
                throw new Exception();
            }

            String index = indexMap.get(a[0]);
            String then = a[1];
            double value = Double.parseDouble(a[2]);
            if (">".equals(then) || ">=".equals(then)) {
                fillGreaterThan(field, sc, index, value);
            } else if ("<".equals(then) || "<=".equals(then)){
                fillLessThan(field, sc, index, value);
            } else {
                throw new Exception();
            }
        }
    }

    public static void fillGreaterThan(String field, StatisticConf sc, String k, double v) {
        if ("pre_condition".equals(field)) {
            sc.putToPreConditionGreaterThan(k, v);
        } else if ("base_condition".equals(field)) {
            sc.putToBaseConditionGreaterThan(k, v);
        }
    }

    public static void fillLessThan(String field, StatisticConf sc, String k, double v) {
        if ("pre_condition".equals(field)) {
            sc.putToPreConditionLessThan(k, v);
        } else if ("base_condition".equals(field)) {
            sc.putToBaseConditionLessThan(k, v);
        }
    }

    public static void main(String[] args) throws Exception {
        Map<String, StatisticConf> map = new LinkedHashMap<>();
        String content = FileTool.readContent("./conf/statics_data.json");

        StatisticConfParser.parse(content, map);

        for (Map.Entry<String, StatisticConf> entry : map.entrySet()) {

        }
    }
}
