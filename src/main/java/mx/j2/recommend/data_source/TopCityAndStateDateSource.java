package mx.j2.recommend.data_source;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import mx.j2.recommend.conf.Conf;
import mx.j2.recommend.util.FileTool;
import mx.j2.recommend.util.MXJudgeUtils;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author liangjie.feng
 * @version 1.0
 * @date 2021/2/25 下午3:50
 * @description
 */
public class TopCityAndStateDateSource extends BaseDataSource {
    public static List<String> TOP_CITY_LIST;
    public static List<String> TOP_STATE_LIST;

    static {
        String result = FileTool.readContent(Conf.getTopCityAndStateConfPath());
        if (MXJudgeUtils.isNotEmpty(result)) {
            JSONObject jsonObject = JSON.parseObject(result);
            String topCity = jsonObject.getString("top_city");
            String topState = jsonObject.getString("top_state");
            TOP_CITY_LIST = Arrays.asList(topCity.split(","));
            TOP_STATE_LIST = Arrays.asList(topState.split(","));
        }
    }
}
