package mx.j2.recommend.pool_conf;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.parser.Feature;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.FileTool;
import mx.j2.recommend.util.MXJudgeUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * 预热池池配置文件解析
 */
public class ExposurePoolConfParser {

    public static void parse(String content, Map<String, ExposurePoolConf> outMap) {
        JSONObject poolConfObj = JSONObject.parseObject(content);

        // 解析 base
        ExposurePoolConf basePoolConf = parse(poolConfObj);
        outMap.put("base", basePoolConf);

        // 解析内嵌小流量配置
        if (poolConfObj.containsKey("small_flow")) {
            JSONObject object = poolConfObj.getJSONObject("small_flow");

            if (MXJudgeUtils.isNotEmpty(object)) {
                for (Map.Entry<String, Object> entry : object.entrySet()) {
                    try {
                        JSONObject jo = JSON.parseObject(entry.getValue().toString(), Feature.OrderedField);
                        ExposurePoolConf flowPoolConf = new ExposurePoolConf(basePoolConf);
                        parse(jo, flowPoolConf);
                        outMap.put(entry.getKey(), flowPoolConf);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    public static ExposurePoolConf parse(JSONObject o) {
        ExposurePoolConf pc = new ExposurePoolConf();
        return parse(o, pc);
    }

    public static ExposurePoolConf parse(JSONObject o, ExposurePoolConf pc) {
        if (o.containsKey("es_index")) {
            pc.esIndex = o.getString("es_index");
        }

        if (o.containsKey("rate")) {
            pc.rate = o.getDoubleValue("rate");
        }

        if (o.containsKey("recall_size")) {
            pc.recallSize = o.getIntValue("recall_size");
        }

        if (o.containsKey("description")) {
            pc.description = o.getString("description");
        }

        if (o.containsKey("sort_field")) {
            try {
                pc.sortField = o.getJSONArray("sort_field");
                pc.sortField.forEach((k) -> {
                    JSONObject value = (JSONObject) k;
                    if (MXJudgeUtils.isEmpty(value.values())) {
                        return;
                    }

                    // 这里认为sort里的JSONObject只有一个key
                    value = (JSONObject) value.values().iterator().next();

                    // 检查 order
                    if (value.containsKey("order")) {
                        String orderValue = value.getString("order");
                        if (!"asc".equals(orderValue) && !"desc".equals(orderValue)) {
                            throw new IllegalArgumentException("order value");
                        }
                    } else {// 默认升序
                        value.put("order", "asc");
                    }

                    // 检查 missing
                    if (!value.containsKey("missing")) {
                        value.put("missing", 0.0);
                    }
                    //回写，设置上默认值
                });
                //check the sort
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        return pc;
    }

    public static void main(String[] args) {
        Map<String, ExposurePoolConf> all = new HashMap<>();
        String content = FileTool.readContent("./conf/exposure_pool.json");
        ExposurePoolConfParser.parse(content, all);

        for (Map.Entry<String, ExposurePoolConf> entry : all.entrySet()) {
            System.out.println("flow name: " + entry.getKey());
            System.out.println("flow conf: " + entry.getValue());
            System.out.println("cache key: " + DefineTool.toKey(entry.getValue().esIndex, entry.getValue().sortField.toString()));
        }
    }
}
