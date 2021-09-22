package mx.j2.recommend.data_model.document;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

/**
 * @author ：zhongrenli
 * @date ：Created in 11:33 上午 2020/7/5
 */
@Getter
@Setter
public class ScoreDocument implements Serializable {
    /**
     * 人为添加的分数 0 - 5
     */
    public float baseScore;

    /**
     * 根据 app name 打的分数
     */
    public float appNameScore;

    /**
     * 根据语言打的分数
     */
    public float languageScore;

    /**
     *
     */
    public float minusScore;

    /**
     * 召回器权重分数
     */
    public float recallWeightScore;

    /**
     * 人工置顶的分数，存在于 Redis
     */
    public double manualTopScore;

    /**
     * 策略离线计算的score，存在于 Redis
     */
    public double offlineCalculateScore;

    /**
     * 策略离线计算的score，存在于 Redis
     */
    public double strategyPoolScore;

    /**
     * 小语言视频的score，存在于 Redis
     */
    public double languageCalculateScore;

    public double predictScore;

    public ScoreDocument() {
        init();
    }

    private void init() {
        baseScore = 0f;
        appNameScore = 0f;
        languageScore = 0f;
        minusScore = 0f;
        manualTopScore = 0;
        offlineCalculateScore = 0;
        languageCalculateScore = 0;
        predictScore = 0;
        strategyPoolScore = 0;
    }

    @Override
    public String toString() {
        return "ScoreDocument{" +
                "baseScore=" + baseScore +
                ", appNameScore=" + appNameScore +
                ", languageScore=" + languageScore +
                ", minusScore=" + minusScore +
                ", recallWeightScore=" + recallWeightScore +
                ", manualTopScore=" + manualTopScore +
                ", offlineCalculateScore=" + offlineCalculateScore +
                ", strategyPoolScore=" + strategyPoolScore +
                ", languageCalculateScore=" + languageCalculateScore +
                ", predictScore=" + predictScore +
                '}';
    }
}
