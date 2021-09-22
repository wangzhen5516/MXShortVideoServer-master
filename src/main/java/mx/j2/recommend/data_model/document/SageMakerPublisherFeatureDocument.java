package mx.j2.recommend.data_model.document;

import com.alibaba.fastjson.JSONObject;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.text.NumberFormat;

/**
 * @Author: xiaoling.zhu
 * @Date: 2020-12-18
 */

@Getter
@Setter
public class SageMakerPublisherFeatureDocument implements Serializable {
    private int pubView;
    private double pubLoopPlayRate;
    private double pubListCtr;
    private double pubFinishRate;
    private double pubLikeRate;
    private double pubDownloadRate;
    private double pubShareRate;
    private int pubFollowerAll;
    private int pubTotalVideos;
    private double pubDailyNewVideos;
    private double pubPlayRate;
    private double pubAVGPlayTime;


    private volatile String featureStr;

    public SageMakerPublisherFeatureDocument() {
        pubView = 0;
        pubLoopPlayRate = 0.0;
        pubListCtr = 0.0;
        pubFinishRate = 0.0;
        pubLikeRate = 0.0;
        pubDownloadRate = 0.0;
        pubShareRate = 0.0;
        pubFollowerAll = 0;
        pubTotalVideos = 0;
        pubDailyNewVideos = 0.0;
        pubPlayRate = 0.0;
        pubAVGPlayTime = 0.0;

        featureStr = null;
    }

    private String formatDouble(double d){
        NumberFormat numberFormat = NumberFormat.getInstance();
        numberFormat.setMaximumFractionDigits(20);
        numberFormat.setGroupingUsed(false);
        numberFormat.setMinimumFractionDigits(1);
        return numberFormat.format(d);
    }

    private String getFeatureStr(){
        if(featureStr == null) {
            synchronized (this) {
                if (featureStr == null) {
                    StringBuilder stringBuilder = new StringBuilder();
                    stringBuilder.append(pubView);
                    stringBuilder.append(',');
                    stringBuilder.append(formatDouble(pubLoopPlayRate));
                    stringBuilder.append(',');
                    stringBuilder.append(formatDouble(pubListCtr));
                    stringBuilder.append(',');
                    stringBuilder.append(formatDouble(pubFinishRate));
                    stringBuilder.append(',');
                    stringBuilder.append(formatDouble(pubLikeRate));
                    stringBuilder.append(',');
                    stringBuilder.append(formatDouble(pubDownloadRate));
                    stringBuilder.append(',');
                    stringBuilder.append(formatDouble(pubShareRate));
                    stringBuilder.append(',');
                    stringBuilder.append(pubFollowerAll);
                    stringBuilder.append(',');
                    stringBuilder.append(pubTotalVideos);
                    stringBuilder.append(',');
                    stringBuilder.append(formatDouble(pubDailyNewVideos));
                    stringBuilder.append(',');
                    stringBuilder.append(formatDouble(pubPlayRate));
                    stringBuilder.append(',');
                    stringBuilder.append(formatDouble(pubAVGPlayTime));
                    featureStr = stringBuilder.toString();
                }
            }
        }
        return featureStr;
    }

    @Override
    public String toString() {
        return getFeatureStr();
    }
}
