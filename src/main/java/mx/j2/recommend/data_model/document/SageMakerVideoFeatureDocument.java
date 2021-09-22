package mx.j2.recommend.data_model.document;

import com.alibaba.fastjson.JSONObject;
import lombok.Getter;
import lombok.Setter;
import mx.j2.recommend.util.BaseMagicValueEnum;
import mx.j2.recommend.util.MXStringUtils;

import java.io.Serializable;
import java.text.NumberFormat;

/**
 * @Author: xiaoling.zhu
 * @Date: 2020-12-16
 */

@Getter
@Setter
public class SageMakerVideoFeatureDocument implements Serializable {
    private static final String VIEW_PREFIX = "view_";
    private static final String LIST_CTR_PREFIX = "list_ctr_";
    private static final String VIEW_USER_PREFIX = "view_user_";
    private static final String USER_LIST_CTR_PREFIX = "user_list_ctr_";
    private static final String AVG_PLAY_TIME_PREFIX = "avg_playtime_";
    private static final String PLAY_RATE_PREFIX = "play_rate_";
    private static final String FINISH_RATE_PREFIX = "finish_rate_";
    private static final String LOOP_PLAY_RATE_PREFIX = "loop_play_rate_";
    private static final String LIKE_RATE_PREFIX = "like_rate_";
    private static final String SHARE_RATE_PREFIX = "share_rate_";
    private static final String DOWNLOAD_RATE_PREFIX = "download_rate_";

    private long duration;
    private String appName;
    private String language;

    private DayFeatureInfo featureInfo30D;

    private DayFeatureInfo featureInfo7D;

    private DayFeatureInfo featureInfo1D;


    private volatile String featureStr;

    public SageMakerVideoFeatureDocument() {
        duration = 0;
        appName = BaseMagicValueEnum.STRING_INITIAL_VALUE;
        language = BaseMagicValueEnum.STRING_INITIAL_VALUE;
        featureInfo30D = new DayFeatureInfo();
        featureInfo7D = new DayFeatureInfo();
        featureInfo1D = new DayFeatureInfo();

        featureStr = null;
    }

    public void loadVideoFeatureInfo(JSONObject jsonObject, long duration, String appName, String language){
        this.duration = duration;
        if(MXStringUtils.isNotEmpty(appName)){
            this.appName = appName;
        }
        if(MXStringUtils.isNotEmpty(language)){
            this.language = language;
        }
        if(jsonObject.containsKey(BaseMagicValueEnum.FEATURE30D)){
            JSONObject feature30 = jsonObject.getJSONObject(BaseMagicValueEnum.FEATURE30D);
            if(feature30!=null){
                this.loadFeatureInfoWithDaySuffix(feature30, "30d", this.featureInfo30D);
            }
        }
        if(jsonObject.containsKey(BaseMagicValueEnum.FEATURE7D)){
            JSONObject feature7 = jsonObject.getJSONObject(BaseMagicValueEnum.FEATURE7D);
            if(feature7!=null){
                this.loadFeatureInfoWithDaySuffix(feature7, "7d", this.featureInfo7D);
            }
        }
        if(jsonObject.containsKey(BaseMagicValueEnum.FEATURE1D)){
            JSONObject feature1 = jsonObject.getJSONObject(BaseMagicValueEnum.FEATURE1D);
            if(feature1!=null){
                this.loadFeatureInfoWithDaySuffix(feature1, "1d", this.featureInfo1D);
            }
        }
    }

    private void loadFeatureInfoWithDaySuffix(JSONObject dayFeatureJSON, String suffix, DayFeatureInfo dayFeatureInfo){
        String viewKey = VIEW_PREFIX+suffix;
        if(dayFeatureJSON.containsKey(viewKey)){
            dayFeatureInfo.view = dayFeatureJSON.getIntValue(viewKey);
        }

        String listCtrKey = LIST_CTR_PREFIX+suffix;
        if(dayFeatureJSON.containsKey(listCtrKey)){
            dayFeatureInfo.listCtr = dayFeatureJSON.getDoubleValue(listCtrKey);
        }

        String viewUserKey = VIEW_USER_PREFIX+suffix;
        if(dayFeatureJSON.containsKey(viewUserKey)){
            dayFeatureInfo.viewUser = dayFeatureJSON.getIntValue(viewUserKey);
        }

        String userListCtrKey = USER_LIST_CTR_PREFIX+suffix;
        if(dayFeatureJSON.containsKey(userListCtrKey)){
            dayFeatureInfo.userListCtr = dayFeatureJSON.getDoubleValue(userListCtrKey);
        }

        String avgPlaytimeKey = VIEW_USER_PREFIX+suffix;
        if(dayFeatureJSON.containsKey(avgPlaytimeKey)){
            dayFeatureInfo.avgPlaytime = dayFeatureJSON.getDoubleValue(avgPlaytimeKey);
        }

        String playRateKey = PLAY_RATE_PREFIX+suffix;
        if(dayFeatureJSON.containsKey(playRateKey)){
            dayFeatureInfo.playRate = dayFeatureJSON.getDoubleValue(playRateKey);
        }

        String finishRateKey = FINISH_RATE_PREFIX+suffix;
        if(dayFeatureJSON.containsKey(finishRateKey)){
            dayFeatureInfo.finishRate = dayFeatureJSON.getDoubleValue(finishRateKey);
        }

        String loopPlayRateKey = LOOP_PLAY_RATE_PREFIX+suffix;
        if(dayFeatureJSON.containsKey(loopPlayRateKey)){
            dayFeatureInfo.loopPlayRate = dayFeatureJSON.getDoubleValue(loopPlayRateKey);
        }

        String likeRateKey = LIKE_RATE_PREFIX+suffix;
        if(dayFeatureJSON.containsKey(likeRateKey)){
            dayFeatureInfo.likeRate = dayFeatureJSON.getDoubleValue(likeRateKey);
        }
        String shareRateKey = SHARE_RATE_PREFIX+suffix;
        if(dayFeatureJSON.containsKey(shareRateKey)){
            dayFeatureInfo.shareRate = dayFeatureJSON.getDoubleValue(shareRateKey);
        }
        String downloadRateKey = DOWNLOAD_RATE_PREFIX+suffix;
        if(dayFeatureJSON.containsKey(downloadRateKey)){
            dayFeatureInfo.downloadRate = dayFeatureJSON.getDoubleValue(downloadRateKey);
        }
    }

    private String getfeatureStr(){
        if(featureStr == null){
            synchronized (this){
                if(featureStr == null){
                    StringBuilder stringBuilder = new StringBuilder();
                    stringBuilder.append(duration);
                    stringBuilder.append(',');
                    stringBuilder.append(appName);
                    stringBuilder.append(',');
                    stringBuilder.append(language);
                    stringBuilder.append(',');
                    stringBuilder.append(featureInfo30D.toString());
                    stringBuilder.append(',');
                    stringBuilder.append(featureInfo7D.toString());
                    stringBuilder.append(',');
                    stringBuilder.append(featureInfo1D.toString());
                    featureStr = stringBuilder.toString();
                }
            }
        }
        return featureStr;
    }

    @Getter
    @Setter
    class DayFeatureInfo implements Serializable{
        private int view;
        private double listCtr;
        private int viewUser;
        private double userListCtr;
        private double avgPlaytime;
        private double playRate;
        private double finishRate;
        private double loopPlayRate;
        private double likeRate;
        private double shareRate;
        private double downloadRate;

        public DayFeatureInfo() {
            view = 0;
            listCtr = 0.0;
            viewUser = 0;
            userListCtr = 0.0;
            avgPlaytime = 0.0;
            playRate = 0.0;
            finishRate = 0.0;
            loopPlayRate = 0.0;
            likeRate = 0.0;
            shareRate = 0.0;
            downloadRate = 0.0;
        }

        private String formatDouble(double d){
            NumberFormat numberFormat = NumberFormat.getInstance();
            numberFormat.setMaximumFractionDigits(20);
            numberFormat.setGroupingUsed(false);
            numberFormat.setMinimumFractionDigits(1);
            return numberFormat.format(d);
        }

        @Override
        public String toString() {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append(view);
            stringBuilder.append(',');
            stringBuilder.append(formatDouble(listCtr));
            stringBuilder.append(',');
            stringBuilder.append(viewUser);
            stringBuilder.append(',');
            stringBuilder.append(formatDouble(userListCtr));
            stringBuilder.append(',');
            stringBuilder.append(formatDouble(avgPlaytime));
            stringBuilder.append(',');
            stringBuilder.append(formatDouble(playRate));
            stringBuilder.append(',');
            stringBuilder.append(formatDouble(finishRate));
            stringBuilder.append(',');
            stringBuilder.append(formatDouble(loopPlayRate));
            stringBuilder.append(',');
            stringBuilder.append(formatDouble(likeRate));
            stringBuilder.append(',');
            stringBuilder.append(formatDouble(shareRate));
            stringBuilder.append(',');
            stringBuilder.append(formatDouble(downloadRate));
            return stringBuilder.toString();
        }
    }

    @Override
    public String toString() {
        return getfeatureStr();
    }
}
