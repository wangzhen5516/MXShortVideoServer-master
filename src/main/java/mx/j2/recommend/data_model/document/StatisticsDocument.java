package mx.j2.recommend.data_model.document;

import mx.j2.recommend.data_model.statistics_document.BaseStatisticsDocument;
import lombok.Getter;
import lombok.Setter;
import mx.j2.recommend.util.annotation.FilterField;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

/**
 * @author ：zhongrenli
 * @date ：Created in 11:33 上午 2020/7/5
 */
@Getter
@Setter
public class StatisticsDocument implements Serializable {

    private Map<String, BaseStatisticsDocument> map;

    /**
     * like rate
     */
    @FilterField
    public double likeRate30d;

    @FilterField
    public double likeRateRealtime;

    @FilterField
    public double likeRateESPool;

    /**
     * down rate
     */
    @FilterField
    public double downloadRate30d;

    @FilterField
    public double downloadRate3d;

    @FilterField
    public double downloadRateRealtime;

    @FilterField
    public double downloadRateESPool;

    /**
     * share rate
     */
    @FilterField
    public double shareRate30d;
    @FilterField
    public double shareRate3d;

    @FilterField
    public double likeRate0d;

    @FilterField
    public double shareRateRealtime;

    @FilterField
    public double shareRateESPool;

    /**
     * finished rate
     */
    @FilterField
    public double finishedRate30d;

    @FilterField
    public double finishedRateRealtime;

    @FilterField
    public double finishedRateESPool;

    /**
     * play(over threshold[2s]) rate
     */
    @FilterField
    public double playRate30d;

    @FilterField
    public double playRateRealtime;

    @FilterField
    public double playRateESPool;

    /**
     * finish retaintion rate
     */
    @FilterField
    public double finishRetentionSum10s30d;

    @FilterField
    public double score_30d;

    @FilterField
    public double finishRetentionSum10sRealtime;

    @FilterField
    public double finishRetentionSum10sESPool;

    /**
     * view all
     */
    @FilterField
    public double viewAll30d;
    @FilterField
    public int viewAll3d;

    @FilterField
    public int viewAllRealtime;

    @FilterField
    public int viewAllESPool;

    public double finishRate5sCut30d;

    public double avgPlaytime30d;

    /**
     * load successful sign
     */
    public boolean loadSuccess;


    public StatisticsDocument() {
        init();
    }

    private void init() {
        map = new HashMap<>(16);
        shareRate30d = -0.1;
        shareRate3d = 0;
        playRate30d = -0.1;
        downloadRate30d = -0.1;
        downloadRate3d = -0.1;
        finishedRate30d = -0.1;
        likeRate30d = -0.1;
        finishRetentionSum10s30d = -0.1;
        viewAll30d = 0;
        viewAll3d = 0;
        avgPlaytime30d = -0.1;
        likeRateRealtime = -0.1;
        downloadRateRealtime = -0.1;
        shareRateRealtime = -0.1;
        finishedRateRealtime = -0.1;
        playRateRealtime = -0.1;
        finishRetentionSum10sRealtime = -0.1;
        viewAllRealtime = 0;
        loadSuccess = false;
        downloadRateESPool = -0.1;
        shareRateESPool = -0.1;
        finishedRateESPool = -0.1;
        playRateESPool = -0.1;
        finishRetentionSum10sESPool = -0.1;
        viewAllESPool = -1;
        likeRate0d = -0.1;
        score_30d = -0.1;
    }

    public void put(String key, BaseStatisticsDocument v) {
        map.put(key, v);
    }

    public BaseStatisticsDocument get(String k) {
        return map.getOrDefault(k, null);
    }

    public boolean exist(String k) {
        return map.containsKey(k);
    }

    public void printMap() {
        System.out.println(map);
    }


    public String get30dInfo() {
        return "StatisticsDocument{" +
                "likeRate=" + likeRate30d +
                ", downloadRate=" + downloadRate30d +
                ", shareRate=" + shareRate30d +
                ", finishedRate=" + finishedRate30d +
                ", playRate30d=" + playRate30d +
                ", finishRetentionSum10s30d=" + finishRetentionSum10s30d +
                ", viewAll30d" + viewAll30d +
                ", avgPlaytime30d" + avgPlaytime30d +
                '}';
    }

    public String getRealTimeInfo() {
        return "StatisticsDocument{" +
                "likeRateRealtime=" + likeRateRealtime +
                ", downloadRateRealtime=" + downloadRateRealtime +
                ", shareRateRealtime=" + shareRateRealtime +
                ", finishedRateRealtime=" + finishedRateRealtime +
                ", playRateRealtime=" + playRateRealtime +
                ", finishRetentionSum10sRealtime=" + finishRetentionSum10sRealtime +
                ", viewAllRealtime=" + viewAllRealtime +
                '}';
    }

    public static void main(String[] args) {
        StatisticsDocument doc = new StatisticsDocument();
        doc.finishedRate30d = 100;

        try {
            Field field0 = doc.getClass().getDeclaredField("finishedRate30d");
            System.out.println(field0);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        }

        Field[] fields = StatisticsDocument.class.getDeclaredFields();
        for (Field field : fields) {
            if (field.isAnnotationPresent(FilterField.class)) {
                FilterField annotation = field.getAnnotation(FilterField.class);
                System.out.println(annotation.name());
            }
        }
    }
}
