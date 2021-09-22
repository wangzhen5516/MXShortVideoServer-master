package mx.j2.recommend.data_model.flow;

import com.alibaba.fastjson.JSONObject;
import mx.j2.recommend.conf.Conf;
import mx.j2.recommend.util.FileTool;
import mx.j2.recommend.util.MXJudgeUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * @author ：zhongrenli
 * @date ：Created in 3:26 下午 2020/10/15
 */
public class LanguageNameToIdFlowParser {

    public static Map<String, String> parse () {
        String content = FileTool.readContent(Conf.getLanguageNameToIdConfPath());
        JSONObject jsonObject = JSONObject.parseObject(content);

        Map<String, String> map = new HashMap<>();

        if (MXJudgeUtils.isEmpty(jsonObject)) {
            return null;
        }

        jsonObject.forEach((k, v) -> map.put(k, (String) v));

        return map;
    }

    public static void main(String[] args) {
        Map<String, String> result = LanguageNameToIdFlowParser.parse();
        for (Map.Entry<String, String> entry : result.entrySet()) {
            System.out.println(entry.getKey());
            System.out.println(entry.getValue());
        }
    }
}
