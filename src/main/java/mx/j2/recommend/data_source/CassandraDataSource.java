package mx.j2.recommend.data_source;

import com.alibaba.fastjson.JSONObject;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.internal.core.metadata.DefaultEndPoint;
import com.newrelic.api.agent.NewRelic;
import com.newrelic.api.agent.Trace;
import mx.j2.recommend.conf.Conf;
import mx.j2.recommend.hystrix.cassandra.CassandraQueryColumnsCommand;
import mx.j2.recommend.hystrix.cassandra.CassandraQueryRowsCommand;
import mx.j2.recommend.hystrix.cassandra.CassandraQueryStringResultCommand;
import mx.j2.recommend.manager.DataSourceManager;
import mx.j2.recommend.task.CassandraExecutor;
import mx.j2.recommend.util.MXJudgeUtils;
import mx.j2.recommend.util.MXStringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.concurrent.NotThreadSafe;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static java.lang.Thread.sleep;
import static mx.j2.recommend.util.BaseMagicValueEnum.*;


/**
 * @author ：xuejian.zhang
 * @date ：Created in 7:23 下午 2020/07/26
 */
@NotThreadSafe
public class CassandraDataSource extends BaseDataSource {

    private final static Logger logger = LogManager.getLogger(CassandraDataSource.class);

    private final static String keyspace = "takatak";
    // 需要确保用来热身的表一定存在
    private final static String videoTable = "video";
    private final static int batchSize = 100;
    private final static int maxWaitTimeMs = 100;
    private final static String fetchAllByIdQuery = "select * from %s where id='%s' limit 1;";

    private static CqlSession session;
    private static CompletionStage<CqlSession> asyncSession;


    public CassandraDataSource() {
        init();
    }

    private void init() {
        try {
            session = CqlSession.builder()
                    .addContactEndPoint(new DefaultEndPoint(new InetSocketAddress(
                            Conf.getCassandraHostUrl(), Conf.getCassandraHostPort())))
                    .withLocalDatacenter(Conf.getCassandraDc())
                    .withKeyspace(keyspace)
                    .build();
            asyncSession = CqlSession.builder()
                    .addContactEndPoint(new DefaultEndPoint(new InetSocketAddress(
                            Conf.getCassandraHostUrl(), Conf.getCassandraHostPort())))
                    .withLocalDatacenter(Conf.getCassandraDc())
                    .withKeyspace(keyspace)
                    .buildAsync();

            warmupSync();
            warmupAsync();
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e.toString());
        }

        logger.info(this.getClass().getSimpleName() + " init done!");
    }

    @Trace(dispatcher = true)
    public Map<String, JSONObject> fetchDetails(List<String> ids) {
        if (MXJudgeUtils.isEmpty(ids)) {
            return Collections.emptyMap();
        }

        // 去除重复的id
        List<String> idsClone = new ArrayList<>(new HashSet<>(ids));
        List<String> idsCloneReturn = new ArrayList<>(idsClone.size());
        Map<String, JSONObject> responses = new HashMap<>(ids.size());

        int len = idsClone.size();
        int iter = 0;
        while (iter < len) {
            iter += batchSize;
            // 重新初始化新list是为了防止并发问题, 详见subList()方法
            List<String> idsInBatch = new ArrayList<>(idsClone.subList(0, Math.min(idsClone.size(), batchSize)));
            idsClone.removeAll(idsInBatch);
            Map<String, JSONObject> batchResponse = fetchDetailsInBatch(idsInBatch);
            responses.putAll(batchResponse);
            idsCloneReturn.addAll(batchResponse.keySet());
        }
        return responses;
    }

    @Trace(dispatcher = true)
    public Map<String, JSONObject> fetchDetailsInBatch(List<String> ids) {
        if (MXJudgeUtils.isEmpty(ids)) {
            return Collections.emptyMap();
        }

        if (asyncSession == null) {
            NewRelic.noticeError("CassandraDataSource's Async session is null.");
            return Collections.emptyMap();
        }

        long start = System.currentTimeMillis();
        Map<String, JSONObject> responseMap = new ConcurrentHashMap<>(ids.size());
        CountDownLatch counter = new CountDownLatch(ids.size());
        for (String id : ids) {
            if (MXJudgeUtils.isEmpty(id)) {
                counter.countDown();
                continue;
            }
            String query = String.format(fetchAllByIdQuery, videoTable, id);
            CompletionStage<AsyncResultSet> responseStage =
                    asyncSession.thenCompose(
                            session -> {
                                if (session == null) {
                                    return null;// 后面要检查
                                }
                                return session.executeAsync(query);
                            });

            // 必须防一下
            if (responseStage == null) {
                return Collections.emptyMap();
            }

            CompletionStage<JSONObject> resultStage = responseStage.thenApply(
                    resultSet -> {
                        try {
                            // 这里只处理1行, 原因是key-value模式, 只有一行, 如果是其他query, 需要注意下
                            if (0 < resultSet.remaining()) {
                                Row row = resultSet.one();
                                String content = row.getString(CqlIdentifier.fromCql("content"));
                                JSONObject contentJSON = JSONObject.parseObject(content);
                                String mlTags = row.getString(CqlIdentifier.fromCql(ML_TAGS));
                                String statistics = row.getString(CqlIdentifier.fromCql(STATISTICS));
                                String feature30D = row.getString(CqlIdentifier.fromCql(FEATURE30D));
                                String feature7D = row.getString(CqlIdentifier.fromCql(FEATURE7D));
                                String feature3D = row.getString(CqlIdentifier.fromCql(FEATURE3D));
                                String feature1D = row.getString(CqlIdentifier.fromCql(FEATURE1D));
                                String feature0D = row.getString(CqlIdentifier.fromCql(FEATURE0D));
                                int universal = row.getInt(CqlIdentifier.fromCql(UNIVERSAL));
                                float heatScore = row.getFloat(CqlIdentifier.fromCql(HEAT_SCORE));
                                float heatScore2 = row.getFloat(CqlIdentifier.fromCql(HEAT_SCORE2));
                                String attribFilter = row.getString(CqlIdentifier.fromCql(ATTRIB_FILTER));
                                boolean isDuplicated = row.getBoolean(CqlIdentifier.fromCql(IS_DUPLICATED));
                                boolean isIpl = row.getBoolean(CqlIdentifier.fromCql(IS_IPL));
                                boolean isDelete = row.getBoolean(CqlIdentifier.fromCql(IS_DELETE));
                                String bigHead = row.getString(CqlIdentifier.fromCql(BIG_HEAD));
                                String likeInfo = row.getString(CqlIdentifier.fromCql(LIKE_INFO));
                                String permission = row.getString(CqlIdentifier.fromCql(PERMISSION));
                                if (contentJSON != null) {
                                    if (MXStringUtils.isNotEmpty(mlTags)) {
                                        JSONObject mlTagsJSON = JSONObject.parseObject(mlTags);
                                        if (null != mlTagsJSON) {
                                            contentJSON.put(ML_TAGS, mlTagsJSON);
                                        }
                                    }
                                    if (MXStringUtils.isNotEmpty(statistics)) {
                                        JSONObject statisticsObject = JSONObject.parseObject(statistics);
                                        if (null != statisticsObject) {
                                            contentJSON.put(STATISTICS, statisticsObject);
                                        }
                                    }
                                    if (MXStringUtils.isNotEmpty(feature30D)) {
                                        JSONObject feature30DObj = JSONObject.parseObject(feature30D);
                                        if (null != feature30DObj) {
                                            contentJSON.put(FEATURE30D, feature30DObj);
                                        }
                                    }
                                    if (MXStringUtils.isNotEmpty(feature7D)) {
                                        JSONObject feature7DObj = JSONObject.parseObject(feature7D);
                                        if (null != feature7DObj) {
                                            contentJSON.put(FEATURE7D, feature7DObj);
                                        }
                                    }
                                    if (MXStringUtils.isNotEmpty(feature1D)) {
                                        JSONObject feature1DObj = JSONObject.parseObject(feature1D);
                                        if (null != feature1DObj) {
                                            contentJSON.put(FEATURE1D, feature1DObj);
                                        }
                                    }
                                    if (MXStringUtils.isNotEmpty(feature3D)) {
                                        JSONObject feature3DObj = JSONObject.parseObject(feature3D);
                                        if (null != feature3DObj) {
                                            contentJSON.put(FEATURE3D, feature3DObj);
                                        }
                                    }
                                    if (MXStringUtils.isNotEmpty(feature0D)) {
                                        JSONObject feature0DObj = JSONObject.parseObject(feature0D);
                                        if (null != feature0DObj) {
                                            contentJSON.put(FEATURE0D, feature0DObj);
                                        }
                                    }
                                    // TODO 以上这些都要重构
                                    if (MXStringUtils.isNotEmpty(attribFilter)) {
                                        JSONObject filterObj = JSONObject.parseObject(attribFilter);
                                        if (null != filterObj) {
                                            contentJSON.put(ATTRIB_FILTER, filterObj);
                                        }
                                    }
                                    // IS_DUPLICATED  只以外部字段为准
                                    if (isDuplicated) {
                                        contentJSON.put(IS_DUPLICATED, 1);
                                    } else {
                                        contentJSON.put(IS_DUPLICATED, 0);
                                    }
                                    contentJSON.put(UNIVERSAL, universal);
                                    if (MXStringUtils.isNotEmpty(bigHead)) {
                                        JSONObject bigHeadObj = JSONObject.parseObject(bigHead);
                                        if (null != bigHeadObj) {
                                            contentJSON.put(BIG_HEAD, bigHeadObj);
                                        }
                                    }
                                    if (MXStringUtils.isNotEmpty(likeInfo)) {
                                        JSONObject likeInfoObj = JSONObject.parseObject(likeInfo);
                                        if (null != likeInfo) {
                                            contentJSON.put(LIKE_INFO, likeInfoObj);
                                        }
                                    }
                                    contentJSON.put(HEAT_SCORE, heatScore);
                                    contentJSON.put(HEAT_SCORE2, heatScore2);
                                    contentJSON.put(IS_IPL, isIpl);
                                    contentJSON.put(IS_DELETE, isDelete);
                                    contentJSON.put(PERMISSION, permission);
                                }
                                return contentJSON;
                            } else {
                                /**
                                 * TODO  临时先关掉这行日志, 后续上线的时候需要打开
                                 * added by xuejian.zhang, @2020.8.2
                                 */
                                //logger.error(this.getClass().getSimpleName() + " missing content for id : " + id);
                                return null;
                            }
                        } catch (Exception e) {
                            counter.countDown();
                            e.printStackTrace();
                            logger.error(this.getClass().getSimpleName() + " error, get single response from CA cluster issue");
                            return null;
                        }
                    });

            resultStage.whenComplete(
                    (content, error) -> {
                        try {
                            if (null != error) {
                                System.out.printf("Failed to get id & publisher_id: %s%n", error.getMessage());
                            } else if (null != content) {
                                responseMap.put(id, content);
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        } finally {
                            counter.countDown();
                        }
                    });
        }

        try {
            counter.await(maxWaitTimeMs, TimeUnit.MILLISECONDS);
            // long end = System.currentTimeMillis();
            // logger.info("finish batch query, use time : " + String.valueOf(end - start));
        } catch (Exception e) {
            NewRelic.noticeError("error in counter await about maxWaitTimeMs" + e.getMessage());
            e.printStackTrace();
            logger.error(e.getMessage());
            long end = System.currentTimeMillis();
            logger.info("batch query failed, use time : " + String.valueOf(end - start));
        }

        // 防止已经返回了, 但是还有新数据加入进来, 报异常
        Map<String, JSONObject> resultMap = new HashMap<>();
        Iterator<Map.Entry<String, JSONObject>> entries = responseMap.entrySet().iterator();
        while (entries.hasNext()) {
            Map.Entry<String, JSONObject> entry = entries.next();
            resultMap.put(entry.getKey(), entry.getValue());
        }
        return resultMap;
    }

    @Trace(dispatcher = true)
    public Map<String, JSONObject> newFetchDetailsInBatch(List<String> ids) {
        if (MXJudgeUtils.isEmpty(ids)) {
            return Collections.emptyMap();
        }

        if (asyncSession == null) {
            NewRelic.noticeError("CassandraDataSource's Async session is null.");
            return Collections.emptyMap();
        }

        long start = System.currentTimeMillis();
        Map<String, JSONObject> responseMap = new ConcurrentHashMap<>(ids.size());
        CassandraExecutor executor = DataSourceManager.INSTANCE.getCassandraExecutor();
        Set<String> temp = new HashSet<>();
        CountDownLatch counter = new CountDownLatch(ids.size());
        for (String id : ids) {
            if (MXStringUtils.isBlank(id)) {
                counter.countDown();
                continue;
            }

            if (temp.contains(id)) {
                continue;
            }
            temp.add(id);

            String query = String.format(fetchAllByIdQuery, videoTable, id);
            executor.apply(id, query, responseMap, session, counter);
        }

        try {
            counter.await(maxWaitTimeMs, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            NewRelic.noticeError("new error in counter await about maxWaitTimeMs" + e.getMessage());
            e.printStackTrace();
            logger.error(e.getMessage());
            long end = System.currentTimeMillis();
            logger.info("new batch query failed, use time : " + String.valueOf(end - start));
        }

        return responseMap;
    }

    @Trace(dispatcher = true)
    public String getString(String queryFormat, String tableName, String columnName) {
        String query = String.format(queryFormat, tableName);
        return new CassandraQueryStringResultCommand(session, query, columnName).execute();
    }

    /**
     * 通用的返回某行多列值的方法
     *
     * @param queryFormat 查询语句
     * @param table       表名
     * @param columns     列名数组
     * @return 多列的值（按序）
     */
    @Trace(dispatcher = true)
    public List<String> getColumnsOfRow(String queryFormat, String table, List<String> columns) {
        String query = String.format(queryFormat, table);
        return new CassandraQueryColumnsCommand(session, query, columns).execute();
    }

    /**
     * 通用的返回某列多行值的方法
     *
     * @param queryFormat 查询语句
     * @param table       表名
     * @param column      列名
     */
    @Trace(dispatcher = true)
    public List<String> getRowsOfColumn(String queryFormat, String table, String column) {
        String query = String.format(queryFormat, table);
        return new CassandraQueryRowsCommand(session, query, column).execute();
    }

    public String warmupSync() {
        final String[] syncQueries = new String[]{
                "select * from video where id='200001MQv' limit 100;",
                "select * from video where id='200001ZIg' limit 100;",
                "select * from video where id='200001dcH' limit 100;",
                "select * from video where id='15bb2a40f4b995a3c8857eee87c6ce19' limit 100;",
                "select * from video where id='9d61ee7f28f50d8038358f60652b7182' limit 100;",
        };
        final int allTheRowsINeedToFetch = 5;

        final Set<String> responseSet = new HashSet<>();

        final int round = 2;
        for (int m = 0; m < round; m++) {
            for (int i = 0; i < allTheRowsINeedToFetch; i++) {
                ResultSet rs = session.execute(syncQueries[i]);
                Row row = rs.one();
                if (null != row) {
                    responseSet.add(row.toString());
                }
            }
        }
        return String.valueOf(responseSet.size());
    }

    public String warmupAsync() {
        final String[] queries = new String[]{
                "select * from video where id='200001MQv' limit 100;",
                "select * from video where id='200001ZIg' limit 100;",
                "select * from video where id='200001dcH' limit 100;",
                "select * from video where id='15bb2a40f4b995a3c8857eee87c6ce19' limit 100;",
                "select * from video where id='9d61ee7f28f50d8038358f60652b7182' limit 100;",
        };
        final int allTheRowsINeedToFetch = 5;

        final Set<String> responseSet = new HashSet<>();

        final int round = 1;
        final int batch = 2;
        for (int m = 0; m < round; m++) {
            long start = System.currentTimeMillis();
            for (int a = 0; a < batch; a++) {
                for (int i = 0; i < allTheRowsINeedToFetch; i++) {
                    final int j = i;
                    final int n = m;
                    final int b = a;
                    CompletionStage<AsyncResultSet> responseStage =
                            asyncSession.thenCompose(
                                    session -> session.executeAsync(queries[j]));

                    CompletionStage<String> resultStage = responseStage.thenApply(
                            resultSet -> {
                                if (0 < resultSet.remaining()) {
                                    Row row = resultSet.one();
                                    String id = row.getString(CqlIdentifier.fromCql("id"));
                                    String publisher_id = row.getString(CqlIdentifier.fromCql("publisher_id"));
                                    return id + ":" + publisher_id + ":" + String.valueOf(b * 100 + n * 5 + j) + ";";
                                } else {
                                    return "";
                                }
                            });

                    resultStage.whenComplete(
                            (id, error) -> {
                                if (error != null) {
                                    System.out.printf("Failed to get id & publisher_id: %s%n", error.getMessage());
                                } else {
                                    responseSet.add(id);
                                }
                            });
                }
            }
            try {
                sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return String.valueOf(responseSet.size());
    }

}
