package mx.j2.recommend.data_model.flow;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import mx.j2.recommend.conf.Conf;
import mx.j2.recommend.util.FileTool;
import mx.j2.recommend.util.MXJudgeUtils;
import mx.j2.recommend.util.MXStringUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author ：zhongrenli
 * @date ：Created in 2:04 下午 2020/10/15
 */
public class UserSelectLanguageFlowParser {
    /**
     * 解析配置
     */
    public static Map<String, UserSelectLanguageFlow> parse() {
        String content = FileTool.readContent(Conf.getUserSelectLanguageConfPath());
        JSONArray jsonArray = JSONArray.parseArray(content);

        Map<String, UserSelectLanguageFlow> flowMap = new HashMap<>();

        for (int i = 0; i < jsonArray.size(); i++) {
            JSONObject o = jsonArray.getJSONObject(i);

            String languageName = null;
            UserSelectLanguageFlow flow = new UserSelectLanguageFlow();

            if (o.containsKey("language_name_list")) {
                JSONArray array = o.getJSONArray("language_name_list");
                if (MXJudgeUtils.isNotEmpty(array)) {
                    List<String> list = array.toJavaList(String.class);
                    list.sort(String::compareTo);
                    flow.setUserSelectLanguageList(list);
                    languageName = list.toString();
                }
            } else {
                return null;
            }

            if (o.containsKey("combination")) {
                JSONObject object = o.getJSONObject("combination");
                if (MXJudgeUtils.isNotEmpty(object)) {
                    object.forEach((k, v) -> {
                        UserSelectLanguageFlow.LanguageElement e = new UserSelectLanguageFlow.LanguageElement();
                        e.setLanguageName(k);

                        JSONObject valueObject = (JSONObject)v;
                        if (valueObject.containsKey("position")) {
                            e.setPosition(valueObject.getIntValue("position"));
                        }
                        if (valueObject.containsKey("percentage")) {
                            e.setPercentage(valueObject.getDoubleValue("percentage"));
                        }
                        flow.addElementToList(e);
                        flow.addElementToSet(k);
                    });
                }
            } else {
                return null;
            }

            if (MXStringUtils.isBlank(languageName)) {
                return null;
            }
            flow.setLanguageListString(languageName);
            flowMap.put(languageName, flow);
        }
        return flowMap;
    }
}
