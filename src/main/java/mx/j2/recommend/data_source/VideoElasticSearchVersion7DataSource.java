package mx.j2.recommend.data_source;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.newrelic.api.agent.NewRelic;
import com.newrelic.api.agent.Trace;
import mx.j2.recommend.conf.Conf;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.data_collection.OtherDataCollection;
import mx.j2.recommend.data_model.data_collection.info.MXEntityDebugInfo;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_model.interfaces.IDocumentProcessor;
import mx.j2.recommend.hystrix.es.Video7ESRequestCommand;
import mx.j2.recommend.hystrix.es.Video7ESRequestCommandAsync;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.*;
import org.apache.commons.pool2.impl.AbandonedConfig;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseListener;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static mx.j2.recommend.util.BaseMagicValueEnum.SCORE_30D;


/**
 * ElasticSearch数据服务访问类
 *
 * @author zhangxuejian
 */
@ThreadSafe
public class VideoElasticSearchVersion7DataSource extends BaseDataSource {
    private static Logger log = LogManager.getLogger(VideoElasticSearchVersion7DataSource.class);
    private RestClient restClientThreadLocal;

    public static final List<JSONObject> DEFAULT_RESULT_LIST = new ArrayList<>();

    private static final int DEFAULT_ES_CLIENT_NUM = 2000;
    private final static List<BaseDocument> DEFAULT_RECALL_LIST = new ArrayList<BaseDocument>();

    /**
     * 构造函数
     *
     * @param
     */
    public VideoElasticSearchVersion7DataSource() {
        init(DEFAULT_ES_CLIENT_NUM);
    }

    /**
     * 初始化
     */
    public void init(int esClientNum) {
        // TODO : 初始化对象池子, 后续写到配置里
        GenericObjectPoolConfig gopc = new GenericObjectPoolConfig();
        gopc.setMaxTotal(esClientNum);
        gopc.setMaxIdle(esClientNum);
        gopc.setBlockWhenExhausted(Conf.isGenericObjectPoolConfigBlockWhenExhausted());

        this.restClientThreadLocal = RestClient.builder(
                new HttpHost(Conf.getVideoElasticSearchVersion7EndPointURL(), Integer.parseInt(Conf.getVideoElasticSearchVersion7Port()), "http"))
                .setMaxRetryTimeoutMillis(Conf.getRestClientMaxRetryTimeoutMillis())
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                        return httpClientBuilder.setDefaultIOReactorConfig(
                                IOReactorConfig.custom()
                                        .setIoThreadCount(Conf.getRestClientHttpConfigIoThreadCount())
                                        .setSelectInterval(Conf.getRestClientHttpConfigSelectInterval())
                                        .setTcpNoDelay(Conf.isRestClientHttpConfigTcpNoDelay())
                                        .setSoReuseAddress(Conf.isRestClientHttpConfigSoReuseAddress())
                                        .setConnectTimeout(Conf.getRestClientHttpConfigConnectTimeout())
                                        .setSoTimeout(Conf.getRestClientHttpConfigSoTimeout())
                                        .build());
                    }
                })
                .setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback() {
                    @Override
                    public RequestConfig.Builder customizeRequestConfig(RequestConfig.Builder builder) {
                        return builder
                                .setConnectionRequestTimeout(Conf.getRestClientRequestConfigConnectionRequestTimeout())
                                .setConnectTimeout(Conf.getRestClientHttpConfigConnectTimeout())
                                .setSocketTimeout(Conf.getRestClientRequestConfigSocketTimeout());
                    }
                }).build();

        AbandonedConfig abandonedConfig = new AbandonedConfig();
        //borrow 的时候检查泄漏
        abandonedConfig.setRemoveAbandonedOnBorrow(true);
        abandonedConfig.setRemoveAbandonedTimeout(Conf.getGenericObjectPoolConfigRemoveAbandonedTimeout());
        warmup();

        log.info("{\"dataSourceInfo\":\"[VideoElasticSearchVersion7DataSource init successfully]\"}");
    }

    private void warmup() {
        String query = "{ \"size\": 50, \"query\": { \"bool\": { \"must_not\": [ { \"match\": { \"view_privacy\": 2 } } ], \"must\": [ { \"match\": { \"publisher_id\": \"%s\" } }, { \"match\": { \"status\": 1 } } ] } }, \"sort\": [ { \"top_time\": { \"missing\": \"0\", \"order\": \"desc\" } }, { \"order\": { \"missing\": 100000, \"order\": \"asc\" } }, { \"is_ugc_content\": { \"missing\": \"0\", \"order\": \"desc\" } }, { \"online_time\": { \"missing\": \"0\", \"order\": \"desc\" } }, { \"_id\": { \"missing\": \"0\", \"order\": \"desc\" } } ] }";
        String requestUrlFormat = "/%s/_search?pretty=false";
        String INDEX_FORMAT = "takatak_simple_trigger_v%s";
        String[] publishers = {"14919888496557", "12104104584747642104859", "12116638887621847902149", "15659135628363752",
                "12100062619999422721706", "1000301445533", "134deca0ede8e299798116e9f8a5e47bc7", "130229e8f16198f9e467d5a9b849b6c80d", "10000009",
                "13362e9aa6fcb09ab82241eea58935aec6", "1323f37db283f83d2d9988999b9e3cc429", "137388220d3f4704fb4b4a248ff77be122", "15627666094854007", "1000300043800",
                "13ca9132fa8484c103887574ad3936a618", "12113060649078251278079"};

        for (int i = 0; i < 3; i++) {
            BaseDataCollection dc = new OtherDataCollection();
            for (String s : publishers) {
                int hash = s.hashCode() & Integer.MAX_VALUE;
                String indexUrl = String.format(INDEX_FORMAT, hash % 16);
                String elasticSearchRequest = String.format(requestUrlFormat, indexUrl);

                BaseDataCollection.ESRequest esRequest = new BaseDataCollection.ESRequest(
                        elasticSearchRequest,
                        String.format(query, s),
                        "warmup", "", "video");

                dc.videoNewSearchRequestList.add(esRequest);
            }
            try {
                newSearch(dc);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 检索，针对dc中的三类索引文件，对关键词进行检索
     * *
     */
    @Trace(dispatcher = true)
    public void search(BaseDataCollection dc) {
        dc.moduleStartTime = System.nanoTime();
        List<BaseDocument> searchEngineRecallList;
        try {
            searchEngineRecallList = sendAsyncPost(dc);
        } catch (Exception e) {
            String message = String.format("search() Exception -> %s", e.toString());
            LogTool.printErrorLog(log, DefineTool.ErrorNoEnum.NORMAL_ERROR.getErrorNo(), message, null, null);
            searchEngineRecallList = DEFAULT_RECALL_LIST;
        }

        String recallInfo = String.format("total:%s", searchEngineRecallList.size());
        StringBuilder infoSb = new StringBuilder();
        int resultSize = searchEngineRecallList.size();
        if (resultSize <= 0) {
            return;
        }

        for (String recall : dc.searchEngineRecallerSet) {
            int count = 0;
            List<BaseDocument> documentList = new ArrayList<>();

            for (int i = 0; i < resultSize; i++) {
                BaseDocument doc = searchEngineRecallList.get(i);
                if (doc == null) {
                    continue;
                }
                documentList.add(doc);
                count += 1;
            }

            if (dc.localCacheRecallKeyMap.containsKey(recall) && MXJudgeUtils.isNotEmpty(documentList)) {
                String key = dc.localCacheRecallKeyMap.get(recall);
                LocalCacheDataSource localCacheDataSource = MXDataSource.cache();
                localCacheDataSource.setFeedRecallCache(key, documentList);
                dc.syncSearchResultSizeMap.put(recall + "_LocalCache_Push", documentList.size());
            }

            if (dc.localCacheMatchScoreRecallKeyMap.containsKey(recall) && MXJudgeUtils.isNotEmpty(documentList)) {
                String key = dc.localCacheMatchScoreRecallKeyMap.get(recall);
                LocalCacheDataSource localCacheDataSource = MXDataSource.cache();
                localCacheDataSource.setScoreWeightRecallCache(key, documentList, dc.localCacheMatchScoreRecallCacheTimeMap.getOrDefault(key, 20));
                dc.syncSearchResultSizeMap.put(recall + "_ScoreWeightRecallCache_Push", documentList.size());
            }

            infoSb.append(",");
            infoSb.append(recall);
            infoSb.append(":");
            infoSb.append(count);
            dc.syncSearchResultSizeMap.put(recall, count);

        }
        recallInfo += infoSb.toString();
        dc.asynRecallList.addAll(searchEngineRecallList);
        dc.moduleEndTime = System.nanoTime();
        dc.appendToTimeRecord(dc.moduleEndTime - dc.moduleStartTime, "ElasticSearch");
    }

    /**
     * 发送请求
     * *
     */
    @Trace(dispatcher = true)
    private List<BaseDocument> sendAsyncPost(BaseDataCollection dc) {
        final List<BaseDocument> documentList = Collections.synchronizedList(new ArrayList<BaseDocument>());

        List<BaseDataCollection.ESRequest> reqList = dc.videoNewSearchRequestList;
        if (MXJudgeUtils.isEmpty(reqList)) {
            return documentList;
        }

        final CountDownLatch latch = new CountDownLatch(reqList.size());
        try {
            for (BaseDataCollection.ESRequest request : reqList) {
                final BaseDataCollection.ESRequest finalRequest = request;

                Video7ESRequestCommandAsync commandAsync = new Video7ESRequestCommandAsync(
                        restClientThreadLocal,
                        request.searchRequest,
                        request.searchContent,
                        new ResponseListener() {
                            @Override
                            public void onSuccess(Response response) {
                                HttpEntity entity = response.getEntity();
                                String result = null;
                                if (entity != null) {
                                    try {
                                        result = EntityUtils.toString(entity);
                                    } catch (Exception e) {
                                        result = null;
                                        e.printStackTrace();
                                    }
                                }
                                if (null != result) {
                                    List<String> ids = ESJsonTool.loadOnlyIdList(result);
                                    List<BaseDocument> documents = MXDataSource.details().get(ids, finalRequest.recallName);

                                    documentList.addAll(documents);
                                }
                                latch.countDown();
                            }

                            @Override
                            public void onFailure(Exception exception) {
                                latch.countDown();
                            }
                        });

                if (commandAsync.isCircuitBreakerOpen()) {
                    NewRelic.noticeError(commandAsync.getClass().getSimpleName() + " circuit breaker open!");
                }

                commandAsync.execute();
            }
            long timeout = Conf.getElasticsearchAsynTimeout();
            if (latch.await(timeout, TimeUnit.MILLISECONDS) == false) {
                throw new IOException("listener timeout after waiting for [" + timeout + "] ms");
            }

        } catch (Exception e) {
            documentList.clear();
            e.printStackTrace();
            log.error("catch exception in VideoElasticSearchVersion7DataSource -> " + e.toString());
            NewRelic.noticeError("catch exception in VideoElasticSearchVersion7DataSource -> " + e.toString());
            String reqStr = "";
            for (BaseDataCollection.ESRequest request : reqList) {
                reqStr += request.searchRequest + ":" + request.searchContent + ";";
            }
            String message = String.format("catch exception in VideoElasticSearchVersion7DataSource -> %s, req -> %s", e.toString(), reqStr);
            LogTool.printErrorLog(log, DefineTool.ErrorNoEnum.NORMAL_ERROR.getErrorNo(), message, null, null);
        }
        return documentList;
    }

    /**
     * 取出ES返回字段中的heat_score字段
     *
     * @param result
     * @return
     */
    private Map<String, Integer> loadHeatScore(String result) {
        Map<String, Integer> heatScoreMap = new HashMap<>();
        JSONObject responseJson = JSONObject.parseObject(result);

        if (responseJson.containsKey("hits")) {
            JSONObject hitObject = responseJson.getJSONObject("hits");
            if (hitObject.containsKey("hits")) {
                JSONArray hitArray = hitObject.getJSONArray("hits");
                for (int i = 0; i < hitArray.size(); i++) {
                    JSONObject obj = hitArray.getJSONObject(i);
                    if (!obj.containsKey("_id") ||
                            !obj.containsKey("_source")) {
                        continue;
                    }
                    JSONObject source = obj.getJSONObject("_source");
                    Integer heatScore = 0;
                    if (source.containsKey("heat_score")) {
                        heatScore = source.getInteger("heat_score");
                    }
                    heatScoreMap.put(obj.getString("_id"), heatScore);
                }
            }
        }
        return heatScoreMap;
    }


    /**
     * 取出ES返回字段中的online_time字段, 解决排序问题
     * *
     */
    private Map<String, Long> loadOnlineTime(String result) {
        Map<String, Long> onlineTimeMap = new HashMap<>();
        JSONObject responseJson = JSONObject.parseObject(result);

        if (responseJson.containsKey("hits")) {
            JSONObject hitObject = responseJson.getJSONObject("hits");
            if (hitObject.containsKey("hits")) {
                JSONArray hitArray = hitObject.getJSONArray("hits");
                for (int i = 0; i < hitArray.size(); i++) {
                    JSONObject obj = hitArray.getJSONObject(i);
                    if (!obj.containsKey("_id") ||
                            !obj.containsKey("_source")) {
                        continue;
                    }
                    JSONObject source = obj.getJSONObject("_source");
                    Long onlineTime = 0L;
                    if (source.containsKey("online_time")) {
                        onlineTime = source.getLong("online_time");
                    }
                    onlineTimeMap.put(obj.getString("_id"), onlineTime);
                }
            }
        }
        return onlineTimeMap;
    }

    private String constructSearchContent(String key, int size, List<String> idList, List<String> sourceList) {
        JSONObject content = new JSONObject();
        JSONObject termsFather = new JSONObject();
        JSONObject terms = new JSONObject();

        String termsKey = "_id";
        if (MXJudgeUtils.isNotEmpty(key)) {
            termsKey = key;
        }

        terms.put(termsKey, idList);
        termsFather.put("terms", terms);
        content.put("query", termsFather);
        content.put("size", size);

        if (MXJudgeUtils.isNotEmpty(sourceList)) {
            JSONArray sourceArray = new JSONArray();
            sourceArray.addAll(sourceList);
            content.put("_source", sourceArray);
        }
        return content.toString();
    }

    public int sendSyncOnlyReturnTotal(String request, String content) {
        int retNum = 0;

        try {
            Video7ESRequestCommand command = new Video7ESRequestCommand(restClientThreadLocal, request, content);

            if (command.isCircuitBreakerOpen()) {
                NewRelic.noticeError(command.getClass().getSimpleName() + " circuit breaker open!");
            }

            String result = command.execute();

            if (null != result) {
                JSONObject responseJson = JSONObject.parseObject(result);
                if (responseJson.containsKey("hits")) {
                    JSONObject hitObject = responseJson.getJSONObject("hits");
                    if (hitObject.containsKey("total")) {
                        JSONObject total = hitObject.getJSONObject("total");
                        retNum = total.getInteger("value");
                    }
                }
            }
            return retNum;
        } catch (Exception e) {
            e.printStackTrace();
            log.error("catch exception in VideoElasticSearchVersion7DataSource -> " + e.toString());
        }
        return 0;
    }

    private List<BaseDocument> sendSyncSearch(String request, String content, String recallName) {
        List<BaseDocument> objList = new ArrayList<>();

        try {
            Video7ESRequestCommand command = new Video7ESRequestCommand(restClientThreadLocal, request, content);

            if (command.isCircuitBreakerOpen()) {
                NewRelic.noticeError(command.getClass().getSimpleName() + " circuit breaker open!");
            }

            String result = command.execute();

            if (null != result) {
                List<String> ids = ESJsonTool.loadOnlyIdList(result);
                objList = MXDataSource.details().get(ids, recallName);
            }
        } catch (Exception e) {
            objList.clear();
            e.printStackTrace();
            log.error("catch exception in VideoElasticSearchVersion7DataSource -> " + e.toString());
        }
        return objList;
    }

    public List<JSONObject> sendSyncSearchPure(String request, String content) {
        List<JSONObject> objList = new ArrayList<>();

        try {
            Video7ESRequestCommand command = new Video7ESRequestCommand(restClientThreadLocal, request, content);

            if (command.isCircuitBreakerOpen()) {
                NewRelic.noticeError(command.getClass().getSimpleName() + " circuit breaker open!");
            }

            String result = command.execute();

            if (null != result) {
                objList = ESJsonTool.loadList(result);
            }
        } catch (Exception e) {
            objList.clear();
            e.printStackTrace();
            log.error("catch exception in VideoElasticSearchVersion7DataSource -> " + e.toString());
        }
        return objList;
    }


    @Trace(dispatcher = true)
    private void sendSyncPostForWarmUp(String request, String content) {
        try {
            Video7ESRequestCommand command = new Video7ESRequestCommand(restClientThreadLocal, request, content);

            if (command.isCircuitBreakerOpen()) {
                NewRelic.noticeError(command.getClass().getSimpleName() + " circuit breaker open!");
            }

            String result = command.execute();

            if (null != result) {
                ESJsonTool.loadOnlyIdList(result);
            }
        } catch (Exception e) {
            e.printStackTrace();
            String message = String.format("catch exception in VideoElasticSearchVersion7DataSource when warm up -> %s, request: %s,  content: %s", e.toString(), request, content);
            log.error(message);
        }
    }

    /**
     * 用于可以在初始化时召回的es搜索
     * *
     */
    @Trace(dispatcher = true)
    public List<BaseDocument> searchForDocuments(BaseDataCollection.ESRequest esRequest) {
        if (null == esRequest) {
            return Collections.emptyList();
        }
        return sendSyncSearch(esRequest.searchRequest, esRequest.searchContent, esRequest.recallName);
    }


    /**
     * 检索，针对dc中的三类索引文件，对关键词进行检索
     */
    @Trace(dispatcher = true)
    public void newSearch(BaseDataCollection dc) throws Exception {
        dc.moduleStartTime = System.nanoTime();
        LinkedHashMap<String, JSONObject> resultsFromEs;
        List<BaseDocument> searchEngineRecallList = new ArrayList<>();

        resultsFromEs = newSendAsyncPost(dc, 800);
        if (dc.isTimeout) {
            String msg = String.format("version7 es, timeout: %s, will retry!", 800);
            NewRelic.noticeError(msg);
            throw new Exception();
        }
        if (dc.isError) {
            String msg = "version7 es, error happen, will retry!";
            NewRelic.noticeError(msg);
            throw new Exception();
        }
        if (MXJudgeUtils.isNotEmpty(resultsFromEs.keySet())) {
            LinkedHashMap<String, JSONObject> finalResultsFromEs = resultsFromEs;
            IDocumentProcessor processor = document -> {
                JSONObject fieldYouNeedFromEs = finalResultsFromEs.get(document.id);
                if (fieldYouNeedFromEs != null) {
                    if (fieldYouNeedFromEs.containsKey(SCORE_30D) && fieldYouNeedFromEs.get(SCORE_30D) != null) {
                        document.statisticsDocument.setScore_30d(fieldYouNeedFromEs.getDoubleValue(SCORE_30D));
                    } else {
                        document.statisticsDocument.setScore_30d(-0.1);
                    }
                }
            };
            searchEngineRecallList = getDetail(dc, new ArrayList<>(resultsFromEs.keySet()), processor);
        }
        int resultSize = searchEngineRecallList.size();
        if (resultSize <= 0) {
            return;
        }

        for (String recall : dc.searchEngineRecallerSet) {
            int count = 0;
            List<BaseDocument> documentList = new ArrayList<>();

            for (BaseDocument doc : searchEngineRecallList) {
                if (doc == null) {
                    continue;
                }
                documentList.add(doc);
                count += 1;
            }

            if (dc.localCacheRecallKeyMap.containsKey(recall) && MXJudgeUtils.isNotEmpty(documentList)) {
                String key = dc.localCacheRecallKeyMap.get(recall);
                LocalCacheDataSource localCacheDataSource = MXDataSource.cache();
                localCacheDataSource.setFeedRecallCache(key, documentList);
                dc.syncSearchResultSizeMap.put(recall + "_LocalCache_Push", documentList.size());
            }

            if (dc.localCacheMatchScoreRecallKeyMap.containsKey(recall) && MXJudgeUtils.isNotEmpty(documentList)) {
                String key = dc.localCacheMatchScoreRecallKeyMap.get(recall);
                LocalCacheDataSource localCacheDataSource = MXDataSource.cache();
                localCacheDataSource.setScoreWeightRecallCache(key, documentList, dc.localCacheMatchScoreRecallCacheTimeMap.getOrDefault(key, 20));
                dc.syncSearchResultSizeMap.put(recall + "_ScoreWeightRecallCache_Push", documentList.size());
            }

            dc.syncSearchResultSizeMap.put(recall, count);

        }
        dc.asynRecallList.addAll(searchEngineRecallList);
        dc.moduleEndTime = System.nanoTime();
        dc.appendToTimeRecord(dc.moduleEndTime - dc.moduleStartTime, "ElasticSearch");
    }

    @Trace(dispatcher = true)
    private List<BaseDocument> getDetail(BaseDataCollection dc, List<String> ids, IDocumentProcessor processor) {
        List<BaseDocument> objects = MXDataSource.details().get(ids, "", processor);
        Map<String, String> map = dc.nextTokenMap.getOrDefault(dc.req.interfaceName, null);
        for (BaseDocument object : objects) {
            String id = object.getId();
            object.setRecallName(dc.idToRecallNameMap.get(id));

            // 召回信息
            MXEntityDebugInfo debugInfo = dc.debug.getDebugInfoByEntityId(id);
            debugInfo.recall.name = object.getRecallName();

            if (MXJudgeUtils.isNotEmpty(map) && MXJudgeUtils.isNotEmpty(map.get(id))) {
                object.nextTokenMap.put(dc.req.interfaceName, map.get(id));
            }
        }
        return objects;
    }

    /**
     * 发送请求
     */
    @Trace(dispatcher = true)
    private LinkedHashMap<String, JSONObject> newSendAsyncPost(BaseDataCollection dc, long timeout) {
        LinkedHashMap<String, JSONObject> fromElasticSearch = new LinkedHashMap<>();

        List<BaseDataCollection.ESRequest> reqList = dc.videoNewSearchRequestList;
        if (MXJudgeUtils.isEmpty(reqList)) {
            return fromElasticSearch;
        }

        final CountDownLatch latch = new CountDownLatch(reqList.size());
        try {
            for (BaseDataCollection.ESRequest request : reqList) {
                final BaseDataCollection.ESRequest finalRequest = request;

                Video7ESRequestCommandAsync commandAsync = new Video7ESRequestCommandAsync(
                        restClientThreadLocal,
                        request.searchRequest,
                        request.searchContent,
                        new ResponseListener() {
                            @Override
                            public void onSuccess(Response response) {
                                HttpEntity entity = response.getEntity();
                                String result = null;
                                if (entity != null) {
                                    try {
                                        result = EntityUtils.toString(entity);
                                    } catch (Exception e) {
                                        e.printStackTrace();
                                    }
                                }
                                if (MXStringUtils.isNotEmpty(result)) {
                                    LinkedHashMap<String, JSONObject> fieldYouNeedFromEs = ESJsonTool.loadFieldYouNeedFromEs(result);
                                    synchronized (VideoElasticSearchVersion7DataSource.class) {
                                        fromElasticSearch.putAll(fieldYouNeedFromEs);
                                    }
                                    fieldYouNeedFromEs.keySet().forEach(id -> {
                                        dc.idToRecallNameMap.put(id, finalRequest.recallName);
                                    });
                                    Map<String, String> nextTokenMap = ESJsonTool.loadIdSortMap(result);
                                    dc.nextTokenMap.put(dc.req.interfaceName, nextTokenMap);
                                }
                                latch.countDown();
                                dc.isTimeout = false;
                                dc.isError = false;
                            }

                            @Override
                            public void onFailure(Exception exception) {
                                latch.countDown();
                                dc.isError = true;
                            }
                        });

                if (commandAsync.isCircuitBreakerOpen()) {
                    dc.isError = true;
                    NewRelic.noticeError(commandAsync.getClass().getSimpleName() + " circuit breaker open!");
                    System.out.println("circuit breaker open!");
                }
                commandAsync.execute();
            }
            if (!latch.await(timeout, TimeUnit.MILLISECONDS)) {
                dc.isTimeout = true;
            }
        } catch (Exception e) {
            dc.isError = true;
            e.printStackTrace();
            log.error("catch exception in Version7 new -> " + e.toString());
            NewRelic.noticeError("catch exception in Version7 new -> " + e.toString());
            StringBuilder reqStr = new StringBuilder();
            for (BaseDataCollection.ESRequest request : reqList) {
                reqStr.append(request.searchRequest).append(":").append(request.searchContent).append(";");
            }
            String message = String.format("catch exception in Version7 new -> %s, req -> %s", e.toString(), reqStr.toString());
            LogTool.printErrorLog(log, DefineTool.ErrorNoEnum.NORMAL_ERROR.getErrorNo(), message, null, null);
        }
        return fromElasticSearch;
    }
}
