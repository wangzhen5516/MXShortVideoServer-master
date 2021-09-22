package mx.j2.recommend.data_source;

import com.alibaba.fastjson.JSONObject;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.internal.core.metadata.DefaultEndPoint;
import mx.j2.recommend.conf.Conf;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.hystrix.cassandra.StrategyRealTimeCassandraMultiColumnCommand;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.MXJudgeUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class RealTimeStrategyDataSource extends BaseDataSource {
    private final static Logger logger = LogManager.getLogger(RealTimeStrategyDataSource.class);
    private final static String KEYSPACE = "taka_realtime";
    private final static String DEFAULT_TABLE = "takatak_video";
    private final static String QUERY = "select * from %s where item_id='%s';";
    private final static String DOWNLOAD_COLUMN = "download_pv";
    private final static String LIKE_COLUMN = "like_pv";
    private final static String COMPLETE_COLUMN = "completed_pv";
    private final static String CLICK_COLUMN = "click_pv";
    private final static String SHARE_COLUMN = "share_pv";
    private final static String VIEW_COLUMN = "view_pv";
    private final static int RETENTION_DURATION = 10000; //10秒
    private final static String REQ_FORMAT = "/%s/_search?pretty=false";


    private static CqlSession session;

    public RealTimeStrategyDataSource() {
        init();
    }

    private void init() {
        try {
            session = CqlSession.builder()
                    .addContactEndPoint(new DefaultEndPoint(new InetSocketAddress(
                            Conf.getStrategyRealTimeCassandraHost(), Conf.getStrategyRealTimeCassandraPort())))
                    .withLocalDatacenter(Conf.getStrategyRealTimeCassandraDc())
                    .withKeyspace(KEYSPACE)
                    .build();
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e.toString());
        }
        logger.info(this.getClass().getSimpleName() + " init done!");
    }

    public void setExtraInfo(BaseDocument document) {
        if (null == document) {
            return;
        }

        String query = String.format(QUERY, DEFAULT_TABLE, document.id);
        String[] StringColumns = new String[]{VIEW_COLUMN, DOWNLOAD_COLUMN, LIKE_COLUMN, CLICK_COLUMN, SHARE_COLUMN, COMPLETE_COLUMN,
                "completed_pv_1", "completed_pv_2", "completed_pv_3", "completed_pv_4", "completed_pv_5", "completed_pv_6", "completed_pv_7",
                "completed_pv_8", "completed_pv_9", "completed_pv_10"};

        Long[] longRes;
        try {
            longRes = new StrategyRealTimeCassandraMultiColumnCommand(session, query, StringColumns).execute();
        } catch (Exception e) {
            System.out.println("can't get res from realtimeStrategy CA!");
            return;
        }


        if (longRes == null || longRes.length <= 0) {
            return;
        }
        Map<String, Long> realtimeInfoMap = new HashMap<>();
        Long viewCount = longRes[0];
        document.statisticsDocument.setViewAllRealtime(Math.toIntExact(viewCount));
        realtimeInfoMap.put("viewCount", viewCount);

        double downloadRate = (double) longRes[1] / viewCount;
        document.statisticsDocument.setDownloadRateRealtime(downloadRate);
        realtimeInfoMap.put("downloadCount", longRes[1]);

        double likeRate = (double) longRes[2] / viewCount;
        document.statisticsDocument.setLikeRateRealtime(likeRate);
        realtimeInfoMap.put("likeCount", longRes[2]);

        double playRate = (double) longRes[3] / viewCount;
        document.statisticsDocument.setPlayRateRealtime(playRate);
        realtimeInfoMap.put("playCount", longRes[3]);

        double shareRate = (double) longRes[4] / viewCount;
        document.statisticsDocument.setShareRateRealtime(shareRate);
        realtimeInfoMap.put("shareCount", longRes[4]);

        double finishRate = (double) longRes[5] / viewCount;
        document.statisticsDocument.setFinishedRateRealtime(finishRate);
        realtimeInfoMap.put("completedCount", longRes[5]);

        double[] completeCountsIn10s = new double[10];
        for (int i = 6; i < longRes.length; i++) {
            completeCountsIn10s[i - 6] = (double) longRes[i] / viewCount;
        }
//        document.statisticsDocument.setFinishRetentionSum10sRealtime(dumbCalculator(document, completeCountsIn10s));

        setPoolPerformanceInfo(document, realtimeInfoMap);
    }

    public double dumbCalculator(BaseDocument document, double[] completeCountsIn10s) {
        double retentionSum = 0.0;
        if (document.duration < 3 || completeCountsIn10s.length != 10) {
            return retentionSum;
        }
        if (document.duration >= RETENTION_DURATION) {
            for (int i = 0; i < 10; i++) {
                retentionSum += completeCountsIn10s[i];
            }
        } else {
            int duration = (int) document.duration / 1000;
            for (int i = 0; i < duration; i++) {
                retentionSum += completeCountsIn10s[i];
            }
            double diffRate = (completeCountsIn10s[duration - 3] - completeCountsIn10s[duration]) / 3;
            retentionSum += completeCountsIn10s[duration - 1] * (RETENTION_DURATION / 1000 - duration);
            int sum = 0;
            for (int i = 0; i < RETENTION_DURATION / 1000 - duration + 1; i++) {
                sum += i;
            }
            retentionSum -= diffRate * sum;
        }
        return retentionSum;
    }

    public void setPoolPerformanceInfo(BaseDocument document, Map<String, Long> realtimeMap) {
        if (null == document || StringUtils.isEmpty(document.poolIndex)) {
            return;
        }
        String req = String.format(REQ_FORMAT, document.poolIndex);
        String reqContentFormat = "{\"size\": 1,\"query\": {\"bool\": {\"must\": [{\"match\": {\"_id\": \"%s\"}}]}}}";
        String reqContent = String.format(reqContentFormat, document.id);



        List<JSONObject> resList = MXDataSource.ES().sendSyncSearchPure(req, reqContent);
        if (MXJudgeUtils.isEmpty(resList)) {
            return;
        }

//        List<String> completedPVList = Arrays.asList(new String[]{"completed_pv_1", "completed_pv_2", "completed_pv_3", "completed_pv_4", "completed_pv_5", "completed_pv_6", "completed_pv_7",
//                "completed_pv_8", "completed_pv_9", "completed_pv_10"});

        for (JSONObject res : resList) {
            if (null == res || !res.containsKey("view_pv")) {
                continue;
            }
            Long viewCount = res.getLong("view_pv");
            Long deltaViewCount = realtimeMap.get("viewCount") - viewCount;
            document.statisticsDocument.setViewAllESPool(Math.toIntExact(deltaViewCount));

            if (res.containsKey("download_pv")) {
                document.statisticsDocument.setDownloadRateESPool((double) (realtimeMap.get("downloadCount") - res.getLong("download_pv")) / deltaViewCount);
            }
            if (res.containsKey("like_pv")) {
                document.statisticsDocument.setLikeRateESPool((double) (realtimeMap.get("likeCount") - res.getLong("like_pv")) / deltaViewCount);
            }
            if (res.containsKey("completed_pv")) {
                document.statisticsDocument.setFinishedRateESPool((double) (realtimeMap.get("completedCount") - res.getLong("completed_pv")) / deltaViewCount);
            }
            if (res.containsKey("share_pv")) {
                document.statisticsDocument.setShareRateESPool((double) (realtimeMap.get("shareCount") - res.getLong("share_pv")) / deltaViewCount);
            }
            if (res.containsKey("click_pv")) {
                document.statisticsDocument.setPlayRateESPool((double) (realtimeMap.get("playCount") - res.getLong("click_pv")) / deltaViewCount);
            }
//            double[] rateList = new double[10];
//            boolean isLackField = false;
//            for (int i = 0; i < completedPVList.size(); i++) {
//                if (!res.containsKey(completedPVList.get(i))) {
//                    isLackField = true;
//                    break;
//                }
//                double completedPVRate = (double) res.getLong(completedPVList.get(i)) / viewCount;
//                rateList[i] = completedPVRate;
//            }
//            if(isLackField){
//                document.statisticsDocument.setFinishRetentionSum10sESPool(0.0);
//            }
//            document.statisticsDocument.setFinishRetentionSum10sESPool(dumbCalculator(document,rateList));
        }
    }
}

