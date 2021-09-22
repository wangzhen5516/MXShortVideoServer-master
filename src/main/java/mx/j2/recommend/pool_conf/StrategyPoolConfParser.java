package mx.j2.recommend.pool_conf;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import mx.j2.recommend.util.FileTool;

import java.util.HashMap;
import java.util.Map;

/**
 * @author ：zhongrenli
 * @date ：Created in 8:35 下午 2021/2/1
 */
public class StrategyPoolConfParser {

    public static void parse(String fileName, Map<String, StrategyPoolConf> high, Map<String, StrategyPoolConf> low) {
        String content = FileTool.readContent(fileName);
        JSONArray jsonArray = JSONArray.parseArray(content);

        for (int i = 0; i < jsonArray.size(); i++) {
            StrategyPoolConf conf = new StrategyPoolConf();
            JSONObject object = jsonArray.getJSONObject(i);

            conf.poolIndexPrefix = object.getString("pool_index_prefix");
            conf.priority = object.getIntValue("priority");
            conf.basePercentage = object.getDoubleValue("base_percentage");
            conf.poolRecallSize = object.getIntValue("pool_recall_size");
            conf.poolDescription = object.getString("description");
            conf.poolLevel = object.getJSONArray("pool_level");

            if (object.containsKey("small_flow") && !object.getJSONObject("small_flow").isEmpty()) {
                Map<String, StrategyPoolConf.InternalSmallFlow> temp = packSmallFlow(object.getJSONObject("small_flow"));
                if (!temp.isEmpty()) {
                    conf.smallFlowMap.putAll(temp);
                }
            }

            if (object.containsKey("exclude") && !object.getJSONArray("exclude").isEmpty()) {
                conf.excludeSmallFlowList.addAll(object.getJSONArray("exclude").toJavaList(String.class));
            }

            if (null != conf.poolLevel && conf.poolLevel.contains("high")) {
                high.put(conf.poolIndexPrefix, conf);
            }
            if (null != conf.poolLevel && conf.poolLevel.contains("low")) {
                low.put(conf.poolIndexPrefix, conf);
            }
        }
    }

    private static Map<String, StrategyPoolConf.InternalSmallFlow> packSmallFlow(JSONObject object) {
        Map<String, StrategyPoolConf.InternalSmallFlow> map = new HashMap<>();
        for (Map.Entry o : object.entrySet()) {
            String smallFlowName = (String)o.getKey();
            JSONObject value = (JSONObject) o.getValue();

            StrategyPoolConf.InternalSmallFlow smallFlow = new StrategyPoolConf.InternalSmallFlow();
            smallFlow.smallFlowName = smallFlowName;
            smallFlow.percentage = value.getDoubleValue("percentage");
            smallFlow.poolIndexPrefix = value.getString("pool_index_prefix");

            map.put(smallFlowName, smallFlow);
        }
        return map;
    }

    public static void main(String[] args) {
        Map<String, StrategyPoolConf> high = new HashMap<>();
        Map<String, StrategyPoolConf> low = new HashMap<>();
        StrategyPoolConfParser.parse("./conf/strategy_pool.json", high, low);
        for (Map.Entry<String, StrategyPoolConf> entry : high.entrySet()) {
            System.out.println(entry.getKey());
            System.out.println(entry.getValue());
        }
    }
}
