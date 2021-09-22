package mx.j2.recommend.data_model.flow;

import com.alibaba.fastjson.JSONObject;
import mx.j2.recommend.data_model.interfaces.IParser;
import mx.j2.recommend.util.FileTool;

import java.util.List;
import java.util.Map;

/**
 * 针对接口的配置流解析器
 */
public class RecommendFlowParserIf implements IParser<JSONObject, Map<String, RecommendFlow>> {
    private String interfaceName;

    public RecommendFlowParserIf(final String interfaceName) {
        this.interfaceName = interfaceName;
    }

    @Override
    public Map<String, RecommendFlow> parse(JSONObject configIf) throws Exception {
        // range/flow key 检查
        if (!configIf.containsKey(RecommendFlowParserV2.KEY_FLOW)
                || !configIf.containsKey(RecommendFlowParserV2.KEY_RANGE)) {
            throw new Exception("Invalid key 'range' or 'flow'");
        }

        // 解析小流量 flow 部分
        Map<String, RecommendFlow> flowMapIf = RecommendFlowParserV2.parseFlow(interfaceName, configIf.getJSONObject(RecommendFlowParserV2.KEY_FLOW));

        // 解析小流量 range 部分
        Map<String, List<Range>> rangeMapIf = RecommendFlowParserV2.parseRange(configIf.getJSONObject(RecommendFlowParserV2.KEY_RANGE));

        // range 配置小流量和 flow 配置小流量必须一致
        if (!flowMapIf.keySet().equals(rangeMapIf.keySet())) {
            throw new Exception("Flow groups not matched between 'range' and 'flow'");
        }

        // 各 flow 绑定区间信息
        RecommendFlowParserV2.boundRangeToFlow(flowMapIf, rangeMapIf);

        return flowMapIf;
    }

    public static void main(String[] args) {
        // 测试只需填写需要解析的小流量接口
        String interfaceName = "mx_hot_tab_internal_version_2_0";

        /*
         * 解析并打印
         */
        String filePath = "./conf/flows/" + interfaceName + ".json";
        String content = FileTool.readContent(filePath);
        JSONObject confRoot = JSONObject.parseObject(content);
        RecommendFlowParserIf parserIf = new RecommendFlowParserIf(interfaceName);

        Map<String, RecommendFlow> map = null;
        try {
            map = parserIf.parse(confRoot);
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("Size: " + map.size());
        for (Map.Entry<String, RecommendFlow> entry : map.entrySet()) {
            System.out.println(entry);
        }
    }
}
