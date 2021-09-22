package mx.j2.recommend.data_model.statistics_document;

import lombok.Data;
import mx.j2.recommend.util.annotation.StatisticField;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.Map;

/**
 * @author ：zhongrenli
 * @date ：Created in 4:37 下午 2021/3/31
 */
@Data
public class BaseStatisticsDocument implements Serializable {
    /**
     * 通用属性
     */
    @StatisticField(name = "like_rate")
    private double likeRate;

    @StatisticField(name = "download_rate")
    private double downloadRate;

    @StatisticField(name = "share_rate")
    private double shareRate;

    @StatisticField(name = "finish_rate")
    private double finishRate;

    @StatisticField(name = "play_rate")
    private double playRate;

    @StatisticField(name = "view_all")
    private double viewAll;

    @StatisticField(name = "finish_rate_5s_cut")
    private double finishRate5sCut;

    @StatisticField(name = "finish_retention_sum_10s")
    private double finishRetentionSum10s;

    public static void main(String[] args) throws Exception{
        Field[] fields = BaseStatisticsDocument.class.getDeclaredFields();
        for (Field field : fields) {
            if (field.isAnnotationPresent(StatisticField.class)) {
                StatisticField annotation = field.getAnnotation(StatisticField.class);
                InvocationHandler h = Proxy.getInvocationHandler(annotation);
                Field hField = h.getClass().getDeclaredField("memberValues");
                hField.setAccessible(true);
                Map memberValues = (Map)hField.get(h);
                memberValues.put("name", field.getName());
            }
        }
    }

}
