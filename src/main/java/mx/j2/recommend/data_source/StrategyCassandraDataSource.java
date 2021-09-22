package mx.j2.recommend.data_source;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.internal.core.metadata.DefaultEndPoint;
import com.netflix.hystrix.HystrixCommand;
import com.newrelic.api.agent.NewRelic;
import com.newrelic.api.agent.Trace;
import mx.j2.recommend.conf.Conf;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_model.document.SageMakerPublisherFeatureDocument;
import mx.j2.recommend.data_model.interfaces.IDocumentProcessor;
import mx.j2.recommend.hystrix.cassandra.*;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.recall.impl.BaseRecall;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXJudgeUtils;
import mx.j2.recommend.util.MXStringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.concurrent.NotThreadSafe;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@NotThreadSafe
public class StrategyCassandraDataSource extends BaseDataSource {

    private final static Logger logger = LogManager.getLogger(CassandraDataSource.class);

    private final static String KEYSPACE = "takatak";

    private final static String HISTORY_KEYSPACE = "takatak_history";
    private final static String historyListTable = "history";
    private final static String getHistoryByUserIdQuery = "select * from %s where user_id='%s';";

    private final static String getStrategyOutPutByUserIdQuery = "select * from %s where id='%s';";

    private final static String getStrategyByPublisherIdQuery = "select * from %s where publisher_id='%s';";

    private final static String GET_STRATEGY_OUTPUT_BY_UUID_QUERY = "select * from %s where uuid='%s';";
    private final static String LONG_TERM_DISLIKE_PUB_TABLE = "user_dislike_publisher";
    private final static String REAL_TIME_ACTION_TABLE = "item_reco_sw_01";
    private final static String PUBLISHER_FEATURE_TABLE = "publisher_info_d1";
    private final static String SIMILAR_PUBLISH_FOLLOWERS = "act_pub_cf_simi_pub";
    private final static String PUB_VIDEO_NUM_TABLE = "pub_video_num";
    private final static String COLUMN_NAME = "dislike";
    private final static String COLUMN_NAME_REAL_TIME_ACTION = "reco";

    private final static String DEFAULT_UUID = "default_uuid";
    private final static Map<String, JSONObject> EMPTY_RES_MAP = new HashMap<>();

    private static CqlSession session;

    private static CqlSession historySession;

    public StrategyCassandraDataSource() {
        init();
    }

    private void init() {
        try {
            session = CqlSession.builder()
                    .addContactEndPoint(new DefaultEndPoint(new InetSocketAddress(
                            Conf.getStrategyCassandraHostUrl(), Conf.getStrategyCassandraHostPort())))
                    .withLocalDatacenter(Conf.getStrategyCassandraDc())
                    .withKeyspace(KEYSPACE)
                    .build();
            historySession = CqlSession.builder()
                    .addContactEndPoint(new DefaultEndPoint(new InetSocketAddress(
                            Conf.getStrategyCassandraHostUrl(), Conf.getStrategyCassandraHostPort())))
                    .withLocalDatacenter(Conf.getStrategyCassandraDc())
                    .withKeyspace(HISTORY_KEYSPACE)
                    .build();
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e.toString());
        }

        logger.info(this.getClass().getSimpleName() + " init done!");
    }

    @Trace(dispatcher = true)
    public String getStrategyOutPutFromCassandraById(String userId, String tableName) {
        String query = String.format(getStrategyOutPutByUserIdQuery, tableName, userId);
        return new StrategyCassandraQueryStringResultCommand(session, query, "reco").execute();
    }

    @Deprecated
    @Trace(dispatcher = true)
    public List<String> getUserHistoryList(String userId) {
        String query = String.format(getHistoryByUserIdQuery, historyListTable, userId);
        try {
            HystrixCommand<List<String>> command = new StrategyCassandraQueryListResultCommand(historySession, query, "history_list");
            if (command.isCircuitBreakerOpen()) {
                NewRelic.noticeError(command.getCommandKey().name() + " circuit breaker open!");
            }
            return command.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @Trace(dispatcher = true)
    private List<String> getHistoryValueFromCassandraByQuery(String query) {
        ResultSet rs = historySession.execute(query);
        Iterator<Row> rsIterator = rs.iterator();
        if (rsIterator.hasNext()) {
            Row row = rsIterator.next();
            return row.getList("history_list", String.class);
        }
        return null;
    }

    //如果从cassandra搜索不到uuid，用defaultid
    @Trace(dispatcher = true)
    public void getStrategyOutputforTrendingByDefaultUuid(BaseRecall recall, BaseDataCollection baseDc, String tableName) {
        String cacheKey = String.format("%s_%s_%s", recall.getName(), baseDc.req.getInterfaceName(), DEFAULT_UUID);

        LocalCacheDataSource localCacheDataSource = MXDataSource.cache();
        List<BaseDocument> cacheDocumentList = localCacheDataSource.getScoreWeightRecallCache(cacheKey);
        if (MXJudgeUtils.isNotEmpty(cacheDocumentList)) {
            baseDc.userProfileOfflineRecommendList.addAll(cacheDocumentList);
            baseDc.resultFromMap.put(recall.getName(), DefineTool.RecallFrom.LOCAL.getName());
            baseDc.syncSearchResultSizeMap.put(recall.getName(), cacheDocumentList.size());
            return;
        }
        String result = getStrategyOutPutFromCassandraById(DEFAULT_UUID, tableName);
        if (MXStringUtils.isEmpty(result)) {
            return;
        }
        processResultFromCassandra(result, recall, cacheKey, 1800, baseDc);
    }

    //根据从cassandra得到的result做后续处理
    public void processResultFromCassandra(String result, BaseRecall recall, String cacheKey, int cacheTime, BaseDataCollection baseDc) {
        if (MXStringUtils.isEmpty(result)) {
            return;
        }
        JSONArray jsonArray = JSON.parseArray(result);
        Map<String, Double> scoreMap = new HashMap<>();
        for (int i = 0; i < jsonArray.size(); i++) {
            JSONObject jsonObject = (JSONObject) jsonArray.get(i);
            if (jsonObject != null && MXStringUtils.isNotEmpty(jsonObject.getString("id")) && jsonObject.getDouble("score") != null) {
                scoreMap.put(jsonObject.getString("id"), jsonObject.getDouble("score"));
            }
        }

        IDocumentProcessor processor = document -> document.scoreDocument.manualTopScore = scoreMap.get(document.id);

        //根据视频id获取视频详情
        List<BaseDocument> mergedList = MXDataSource.details().get(scoreMap.keySet(), recall.getName(), processor);

        //根据score排序
        mergedList.sort((doc0, doc1) -> Double.compare(doc1.scoreDocument.manualTopScore, doc0.scoreDocument.manualTopScore));
        MXDataSource.cache().setScoreWeightRecallCache(cacheKey, mergedList, cacheTime);
        baseDc.userProfileTrendingOfflineRecommendList.addAll(mergedList);
        baseDc.syncSearchResultSizeMap.put(recall.getName(), mergedList.size());
        baseDc.resultFromMap.put(recall.getName(), DefineTool.RecallFrom.ES.getName());
    }

    /**
     * 设置用户长期不喜欢的publisher列表
     *
     * @param dc
     * @param length
     */
    public void setUserLongTermDislikePublisherList(BaseDataCollection dc, int length) {
        List<String> pubList = getLongTermDislikePubliserList(dc.client.user.uuId, length);

        if (MXJudgeUtils.isNotEmpty(pubList)) {
            dc.longTermuserDislikePubIds = pubList;
        }
    }

    /**
     * 得到用户长期不喜欢的publisher列表
     *
     * @param uuId
     * @param length
     * @return
     */
    private List<String> getLongTermDislikePubliserList(String uuId, int length) {
        String query = String.format(GET_STRATEGY_OUTPUT_BY_UUID_QUERY, LONG_TERM_DISLIKE_PUB_TABLE, uuId);
        String dislikeString = new StrategyCassandraQueryStringResultCommand(session, query, COLUMN_NAME).execute();
        if (MXStringUtils.isEmpty(dislikeString)) {
            return null;
        }

        JSONArray pubListJsonArray = JSONArray.parseArray(dislikeString);
        if (MXJudgeUtils.isEmpty(pubListJsonArray)) {
            return null;
        }

        List<String> pubList = new ArrayList<>();
        for (int i = 0; i < Math.min(length, pubListJsonArray.size()); i++) {
            JSONObject obj = (JSONObject) pubListJsonArray.get(i);
            if (obj != null && obj.containsKey("id") && MXStringUtils.isNotEmpty(obj.getString("id"))) {
                String pubId = obj.getString("id");
                if (!pubList.contains(pubId)) {
                    pubList.add(pubId);
                }
            }
        }
        return pubList;
    }

    public List<String> getRealTimeActionVideoIdList(String videoId) {
        String query = String.format(getStrategyOutPutByUserIdQuery, REAL_TIME_ACTION_TABLE, videoId);
        String videoIdRawData = new StrategyCassandraQueryVideoForRealTimeActionCommand(session, query, COLUMN_NAME_REAL_TIME_ACTION).execute();
        if (MXStringUtils.isEmpty(videoIdRawData)) {
            return null;
        }

        JSONArray videoJsonArray = JSONArray.parseArray(videoIdRawData);
        if (MXJudgeUtils.isEmpty(videoJsonArray)) {
            return null;
        }

        List<String> videoList = new ArrayList<>();
        videoJsonArray.forEach(e -> {
            JSONObject elementObject = (JSONObject) e;
            if (MXJudgeUtils.isEmpty(elementObject)) {
                return;
            }

            videoList.add(elementObject.getString("id"));
        });
        return videoList;
    }

    public Map<String, SageMakerPublisherFeatureDocument> getPubFeatureInPubInfoD1FromStgCa(Set<String> publisherIDs) {
        Map<String, SageMakerPublisherFeatureDocument> totalResult = new HashMap<>(200);
        if (MXJudgeUtils.isNotEmpty(publisherIDs)) {
            Map<String, SageMakerPublisherFeatureDocument> docFromCache = MXDataSource.cache().getSageMakerPubFeatureFromCache(publisherIDs);
            if (docFromCache != null) {
                publisherIDs.removeAll(docFromCache.keySet());
                totalResult.putAll(docFromCache);
            }
            Map<String, SageMakerPublisherFeatureDocument> resultMap = new ConcurrentHashMap<>();
            publisherIDs.parallelStream().forEach((publisherID) -> {
                try {
                    String queryStr = String.format(getStrategyByPublisherIdQuery, PUBLISHER_FEATURE_TABLE, publisherID);
                    StrategyCassandraQueryRowResultCommand pubFeatureCommand = new StrategyCassandraQueryRowResultCommand(session, queryStr);
                    Row row = pubFeatureCommand.execute();
                    if (row != null) {
                        SageMakerPublisherFeatureDocument pubFeatureDoc = new SageMakerPublisherFeatureDocument();

                        if (!row.isNull("view_pv")) {
                            pubFeatureDoc.setPubView(row.getInt("view_pv"));
                        }

                        if (!row.isNull("loop_play_rate")) {
                            pubFeatureDoc.setPubLoopPlayRate(row.getFloat("loop_play_rate"));

                        }

                        if (!row.isNull("list_ctr")) {
                            pubFeatureDoc.setPubListCtr(row.getFloat("list_ctr"));

                        }

                        if (!row.isNull("finish_rate")) {
                            pubFeatureDoc.setPubFinishRate(row.getFloat("finish_rate"));

                        }

                        if (!row.isNull("like_rate")) {
                            pubFeatureDoc.setPubLikeRate(row.getFloat("like_rate"));

                        }

                        if (!row.isNull("download_rate")) {
                            pubFeatureDoc.setPubDownloadRate(row.getFloat("download_rate"));

                        }

                        if (!row.isNull("share_rate")) {
                            pubFeatureDoc.setPubShareRate(row.getFloat("share_rate"));

                        }

                        if (!row.isNull("total_follower")) {
                            pubFeatureDoc.setPubFollowerAll(row.getInt("total_follower"));

                        }

                        if (!row.isNull("video_count")) {
                            pubFeatureDoc.setPubTotalVideos(row.getInt("video_count"));

                        }

                        if (!row.isNull("daily_new_videos")) {
                            pubFeatureDoc.setPubDailyNewVideos(row.getFloat("daily_new_videos"));

                        }

                        if (!row.isNull("play_rate")) {
                            pubFeatureDoc.setPubPlayRate(row.getFloat("play_rate"));

                        }

                        if (!row.isNull("avg_playtime")) {
                            pubFeatureDoc.setPubAVGPlayTime(row.getFloat("avg_playtime"));

                        }

                        resultMap.put(publisherID, pubFeatureDoc);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
            MXDataSource.cache().setSageMakerPublisherFeatureToCache(resultMap);
            totalResult.putAll(resultMap);
        }
        return totalResult;
    }

    public List<String> getSimilarFollowersIds(String publisherId) {
        String query = String.format(getStrategyOutPutByUserIdQuery, SIMILAR_PUBLISH_FOLLOWERS, publisherId);
        String relateData = new CassandraQueryStringResultCommand(session, query, "reco").execute();
        if (MXStringUtils.isBlank(relateData)) {
            return null;
        }
        JSONArray relateArray = JSONArray.parseArray(relateData);
        if (MXJudgeUtils.isEmpty(relateArray)) {
            return null;
        }
        List<String> relateIds = new ArrayList<>(relateArray.size());
        //StringBuffer relateIds=new StringBuffer();
        for (int i = 0; i < relateArray.size(); ++i) {
            JSONObject obj = relateArray.getJSONObject(i);
            if (obj == null) {
                continue;
            }
            if (obj.containsKey("id")) {
                relateIds.add(obj.getString("id"));
            }
        }
        return relateIds;
    }

    public Integer getVideoNumOfPub(String publisherId, String type) {
        String query = String.format(getStrategyOutPutByUserIdQuery, PUB_VIDEO_NUM_TABLE, publisherId);
        String rowType = "";
        if ("public".equals(type)) {
            rowType = "public_num";
        } else if ("private".equals(type)) {
            rowType = "private_num";
        }
        String videoNum = new CassandraQueryStringResultCommand(session, query, rowType).execute();
        if (MXStringUtils.isBlank(videoNum) || !MXStringUtils.isNumeric(videoNum)) {
            return null;
        }
        return Integer.parseInt(videoNum);
    }
}
