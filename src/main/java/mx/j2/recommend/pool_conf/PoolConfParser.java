package mx.j2.recommend.pool_conf;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.parser.Feature;
import mx.j2.recommend.util.FileTool;
import mx.j2.recommend.util.MXJudgeUtils;

import javax.annotation.concurrent.ThreadSafe;
import java.util.*;

/**
 * 流量池配置文件解析
 *
 * @author zhongren.li
 */
@ThreadSafe
public class PoolConfParser {

    public static void parsePoolConfFile(String fileName, Map<String, Map<String, PoolConf>> highLevelMap, Map<String, Map<String, PoolConf>> lowLevelMap) throws Exception {
        parsePoolConfContent(FileTool.readContent(fileName), highLevelMap);
    }

    public static void parsePoolConfContent(String content, Map<String, Map<String, PoolConf>> allLevelMap) throws Exception {
        JSONArray jsonArray = JSONArray.parseArray(content);
        for (Object o : jsonArray) {
            JSONObject pool = (JSONObject) o;
            PoolConf pc = parse(pool);
            // spit to many
            Map<String, PoolConf> smallFlowToPoolConf = new HashMap<>();
            smallFlowToPoolConf.put("base", pc);
            if (pool.containsKey("small_flow")) {
                JSONObject object = pool.getJSONObject("small_flow");
                if (MXJudgeUtils.isNotEmpty(object)) {
                    for (Map.Entry<String, Object> entry : object.entrySet()) {
                        try {
                            JSONObject jo = JSON.parseObject(entry.getValue().toString(), Feature.OrderedField);
                            PoolConf pc1 = new PoolConf(pc);
                            pc1 = parse(jo, pc1);
                            smallFlowToPoolConf.put(entry.getKey(), pc1);
                        } catch (Exception e) {
                            System.out.println("PoolConfParser " + entry.getValue().toString());
                        }
                    }
                }
            }
            // read end

            // set to map
            String name = pc.poolIndex;
            if (pc.poolLevel != null) {
                allLevelMap.put(String.valueOf(pc.priority), smallFlowToPoolConf);
            } else {
                throw new Exception();
            }

        }
    }

    public static PoolConf parse(JSONObject o) {
        PoolConf pc = new PoolConf();
        return parse(o, pc);
    }

    public static PoolConf parse(JSONObject o, PoolConf pc) {
        //read start
        if (o.containsKey("pool_index")) {
            pc.poolIndex = o.getString("pool_index");
        }

        if (o.containsKey("pool_level")) {
            pc.poolLevel = o.getJSONArray("pool_level");
        }

        if (o.containsKey("user_level")) {
            pc.userLevel = o.getJSONArray("user_level");
        }

        if (o.containsKey("ignore_filter")) {
            pc.ignoreFilter = o.getJSONArray("ignore_filter");
        }

        if (o.containsKey("general_tags")) {
            JSONArray array = o.getJSONArray("general_tags");
            if (MXJudgeUtils.isNotEmpty(array)) {
                pc.generalTags = array;
            }
        }

        if (o.containsKey("niche_tags")) {
            JSONArray array = o.getJSONArray("niche_tags");
            if (MXJudgeUtils.isNotEmpty(array)) {
                pc.nicheTags = array;
            }
        }

        if (o.containsKey("percentage")) {
            pc.percentage = o.getDoubleValue("percentage");
        }

        if (o.containsKey("pool_recall_size")) {
            pc.poolRecallSize = o.getIntValue("pool_recall_size");
        }

        if (o.containsKey("priority")) {
            pc.priority = o.getIntValue("priority");
        }

        if (o.containsKey("description")) {
            pc.poolDescription = o.getString("description");
        }

        if (o.containsKey("is_tophot_history")) {
            pc.isTophotHistory = o.getBooleanValue("is_tophot_history");
        }

        if (o.containsKey("is_build_flow_content")) {
            pc.isBuildFlowContent = o.getBooleanValue("is_build_flow_content");
        }

        if (o.containsKey("sort_field")) {
            pc.sortField = o.getString("sort_field");
        }
        if (o.containsKey("sort_field_new")) {
            try {
                pc.sortFieldNew = o.getJSONArray("sort_field_new");
                pc.sortFieldNew.forEach((k) -> {
                    JSONObject value = (JSONObject) k;
                    if(value.values().size() < 1) {
                        return;
                    }
                    // 这里认为sort里的JSONObject只有一个key
                    value = (JSONObject) value.values().iterator().next();

                    // 检查 order
                    if(value.containsKey("order")) {
                        String orderValue = value.getString("order");
                        if(!"asc".equals(orderValue) && !"desc".equals(orderValue)) {
                            throw new IllegalArgumentException("sort_filed_new下的order值有问题，请使用asc或者desc ");
                        }
                    } else {
                        value.put("order", "asc");
                    }
                    // 检查 missing
                    if(!value.containsKey("missing")) {
                        value.put("missing", 0.0);
                    }
                    //回写，设置上默认值
                });
                //check the sort
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("please check the pool.json : "+ o);
            }
        } else {
            //TODO 兼容代码，后期可以删掉，并去掉pool.json中的sort_field
            if(MXJudgeUtils.isNotEmpty(pc.sortField)) {
                String s = String.format("[{\"%s\":{\"order\":\"asc\", \"missing\": 0.0}}]", pc.sortField);
                pc.sortFieldNew = JSON.parseArray(s);
            }
        }
        return pc;
    }

    public static void main(String[] args) throws Exception {
        Map<String, Map<String, PoolConf>> all = new LinkedHashMap<>();
        //PoolConfParser.parsePoolConfFile("./conf/pool.json", high, low);

        String content = FileTool.readContent("./conf/pool.json");
        content = content.replaceAll("\\s+", "");// 替换掉所有空格
        //content = content.replaceAll("\"", "'");// 双引号改单引号
        System.out.println("Generate config content(Copy to Cassandra):");
        System.out.println(content);

        System.out.println("=========================================================================================");

        System.out.println("Current timestamp(Copy to Cassandra): " + System.currentTimeMillis());

        System.out.println("=========================================================================================");

        PoolConfParser.parsePoolConfContent(content, all);

        for (Map.Entry<String, Map<String, PoolConf>> entry : all.entrySet()) {
            System.out.println(entry.getKey());
            System.out.println(entry.getValue());
        }

        System.out.println("=========================================================================================");
        Map<String, Map<String, PoolConf>> map = all;
        List<Map.Entry<String, Map<String, PoolConf>>> list = new ArrayList<>(map.entrySet());
        list.sort((o1, o2) -> Integer.parseInt(o2.getKey()) - Integer.parseInt(o1.getKey()));
        map.clear();
        for (Map.Entry<String, Map<String, PoolConf>> entry : list) {
            map.put(entry.getKey(), entry.getValue());
        }
    }
}
