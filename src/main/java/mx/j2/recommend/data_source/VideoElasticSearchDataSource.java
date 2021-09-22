package mx.j2.recommend.data_source;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.newrelic.api.agent.NewRelic;
import com.newrelic.api.agent.Trace;
import mx.j2.recommend.conf.Conf;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_model.document.ShortDocument;
import mx.j2.recommend.data_model.interfaces.IDocumentProcessor;
import mx.j2.recommend.hystrix.es.ESRequestCommand;
import mx.j2.recommend.hystrix.es.VideoESRequestCommand;
import mx.j2.recommend.hystrix.es.VideoESRequestCommandAsync;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.manager.MXManager;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.ESJsonTool;
import mx.j2.recommend.util.LogTool;
import mx.j2.recommend.util.MXJudgeUtils;
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

import static mx.j2.recommend.util.DefineTool.CategoryEnum.SHORT_VIDEO;
import static mx.j2.recommend.util.DefineTool.FlowInterface.*;


/**
 * ElasticSearch数据服务访问类
 *
 * @author zhangxuejian
 */
@ThreadSafe
public class VideoElasticSearchDataSource extends BaseDataSource {
    private static Logger log = LogManager.getLogger(VideoElasticSearchDataSource.class);
    private RestClient restClientThreadLocal;

    public static final List<JSONObject> DEFAULT_RESULTLIST = new ArrayList<>();

    private static final int DEFAULT_ES_CLIENT_NUM = 2000;
    private final static List<BaseDocument> DEFAULT_RECALLLIST = new ArrayList<BaseDocument>();

    /**
     * 构造函数
     *
     * @param
     */
    public VideoElasticSearchDataSource() {
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
                new HttpHost(Conf.getVideoElasticSearchEndPointURL(), Integer.parseInt(Conf.getVideoElasticSearchEndPointPort()), "http"))
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

        log.info("{\"dataSourceInfo\":\"[VideoElasticSearchSource init successfully]\"}");
    }

    /**
     * 检索，针对dc中的三类索引文件，对关键词进行检索
     * *
     */
    @Trace(dispatcher = true)
    public void search(BaseDataCollection dc) {
        dc.moduleStartTime = System.nanoTime();
        List<String> ids;
        List<BaseDocument> searchEngineRecallList = new ArrayList<>();
        try {
            ids = sendAsyncPost(dc);
            if (MXJudgeUtils.isNotEmpty(ids)) {
                searchEngineRecallList = getDetail(dc, ids);
            }
        } catch (Exception e) {
            String message = String.format("search() Exception -> %s", e.toString());
            LogTool.printErrorLog(log, DefineTool.ErrorNoEnum.NORMAL_ERROR.getErrorNo(), message, null, null);
            searchEngineRecallList = DEFAULT_RECALLLIST;
        }

        String recallInfo = String.format("total:%s", searchEngineRecallList.size());
        StringBuilder infoSb = new StringBuilder();
        int resultSize = searchEngineRecallList.size();
        if (resultSize <= 0) {
            return;
        }

        //RecallScoreWeightDataSource recallScoreWeightDataSource = MXDataSource.score();

        for (String recall : dc.searchEngineRecallerSet) {
            int count = 0;
            List<BaseDocument> documentList = new ArrayList<>();

            for (int i = 0; i < resultSize; i++) {
                BaseDocument doc = searchEngineRecallList.get(i);
                if (doc == null) {
                    continue;
                }

                //float score = recallScoreWeightDataSource.getRecallScoreWeightMap().getOrDefault(recall, 0f);
                float score = MXManager.recall().getComponentInstance(recall).getRecallWeightScore();
                if (null != recall && recall.equals(doc.recallName)) {
                    doc.scoreDocument.recallWeightScore = Math.max(doc.scoreDocument.recallWeightScore, score);
                    if ("JsStrategyPoolRandomRecall".equals(recall) || "JsStrategyPoolRecall".equals(recall)) {
                        doc.scoreDocument.baseScore = 8;
                    }
                    documentList.add(doc);
                    count += 1;
                }
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

    public List<BaseDocument> getDetail(BaseDataCollection dc, List<String> ids) {
        List<BaseDocument> objects = MXDataSource.details().get(ids, "");
        Map<String, String> map = dc.nextTokenMap.getOrDefault(dc.req.interfaceName, null);
        for (BaseDocument object : objects) {
            String id = object.getId();
            object.setRecallName(dc.idToRecallNameMap.get(id));
            if (MXJudgeUtils.isNotEmpty(map) && MXJudgeUtils.isNotEmpty(map.get(id))) {
                object.nextTokenMap.put(dc.req.interfaceName, map.get(id));
            }
        }
        return objects;
    }

    /**
     * 发送请求
     * *
     */
    @Trace(dispatcher = true)
    private List<String> sendAsyncPost(BaseDataCollection dc) {
        final List<String> idsFromElasticSearch = Collections.synchronizedList(new ArrayList<>());

        List<BaseDataCollection.ESRequest> reqList = dc.videoSearchRequestList;
        if (null == reqList) {
            return idsFromElasticSearch;
        }
        final CountDownLatch latch = new CountDownLatch(reqList.size());
        try {
            for (BaseDataCollection.ESRequest request : reqList) {
                final BaseDataCollection.ESRequest finalRequest = request;

                VideoESRequestCommandAsync commandAsync = new VideoESRequestCommandAsync(
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
                                    Map<String, String> nextTokenMap = ESJsonTool.loadIdSortMap(result);
                                    dc.nextTokenMap.put(dc.req.interfaceName, nextTokenMap);
                                    List<String> ids = new ArrayList<>(nextTokenMap.keySet());
                                    idsFromElasticSearch.addAll(ids);
                                    ids.forEach(id -> {
                                        dc.idToRecallNameMap.put(id, finalRequest.recallName);
                                    });
                                    if (MX_VIDEOS_OF_THE_SAME_AUDIO_VERSION_1_0.getName().equals(dc.req.interfaceName)) {
                                        dc.totalNumber = ESJsonTool.loadOnlyTotalNumber(result);
                                    }
                                    if (INTERNAL_VIDEOS_OF_THE_TAG_VERSION_1_0.getName().equals(dc.req.interfaceName)) {
                                        dc.totalNumber = ESJsonTool.loadOnlyTotalNumber(result);
                                    }
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
            e.printStackTrace();
            log.error("catch exception in videoelasticSearchDataSource -> " + e.toString());
            NewRelic.noticeError("catch exception in videoelasticSearchDataSource -> " + e.toString());
            String reqStr = "";
            for (BaseDataCollection.ESRequest request : reqList) {
                reqStr += request.searchRequest + ":" + request.searchContent + ";";
            }
            String message = String.format("catch exception in elasticSearchDataSource -> %s, req -> %s", e.toString(), reqStr);
            LogTool.printErrorLog(log, DefineTool.ErrorNoEnum.NORMAL_ERROR.getErrorNo(), message, null, null);
        }
        return idsFromElasticSearch;
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
     * 取出ES返回字段中的hashtag_heat字段
     *
     * @param result
     * @return
     */
    private Map<String, Double> loadHashtagHeat(String result) {
        Map<String, Double> hashtagHeatMap = new HashMap<>();
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
                    Double hashtagHeat = 0.0;
                    if (source.containsKey("hashtag_heat")) {
                        hashtagHeat = source.getDouble("hashtag_heat");
                    }
                    hashtagHeatMap.put(obj.getString("_id"), hashtagHeat);
                }
            }
        }
        return hashtagHeatMap;
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

    @Trace(dispatcher = true)
    public int sendSyncOnlyReturnTotal(String request, String content) {
        int retNum = 0;

        try {
            VideoESRequestCommand command = new VideoESRequestCommand(restClientThreadLocal, request, content);

            if (command.isCircuitBreakerOpen()) {
                NewRelic.noticeError(command.getClass().getSimpleName() + " circuit breaker open!");
            }

            String result = command.execute();

            if (null != result) {
                JSONObject responseJson = JSONObject.parseObject(result);
                if (responseJson.containsKey("hits")) {
                    JSONObject hitObject = responseJson.getJSONObject("hits");
                    if (hitObject.containsKey("total")) {
                        JSONObject totalObject = hitObject.getJSONObject("total");
                        if (totalObject.containsKey("value")) {
                            retNum = totalObject.getInteger("value");
                        }
                    }
                }
            }
            return retNum;
        } catch (Exception e) {
            e.printStackTrace();
            log.error("catch exception in elasticSearchDataSource -> " + e.toString());
        }
        return 0;
    }

    private List<BaseDocument> sendSyncSearch(String request, String content, String recallName, IDocumentProcessor processor) {
        List<BaseDocument> objList = new ArrayList<>();

        try {
            VideoESRequestCommand command = new VideoESRequestCommand(restClientThreadLocal, request, content);

            if (command.isCircuitBreakerOpen()) {
                NewRelic.noticeError(command.getClass().getSimpleName() + " circuit breaker open!");
            }

            String result = command.execute();

            if (null != result) {
                List<String> ids = ESJsonTool.loadOnlyIdList(result);
                objList = MXDataSource.details().get(ids, recallName, processor);
            }
        } catch (Exception e) {
            objList.clear();
            e.printStackTrace();
            log.error("catch exception in elasticSearchDataSource -> " + e.toString());
        }
        return objList;
    }

    @Trace(dispatcher = true)
    public List<JSONObject> sendSyncSearchPure(String request, String content) {
        List<JSONObject> objList = new ArrayList<>();

        try {
            VideoESRequestCommand command = new VideoESRequestCommand(restClientThreadLocal, request, content);

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
            log.error("catch exception in elasticSearchDataSource -> " + e.toString());
        }
        return objList;
    }


    @Trace(dispatcher = true)
    private void sendSyncPostForWarmUp(String request, String content) {
        try {
            VideoESRequestCommand command = new VideoESRequestCommand(restClientThreadLocal, request, content);

            if (command.isCircuitBreakerOpen()) {
                NewRelic.noticeError(command.getClass().getSimpleName() + " circuit breaker open!");
            }

            String result = command.execute();

            if (null != result) {
                ESJsonTool.loadOnlyIdList(result);
            }
        } catch (Exception e) {
            e.printStackTrace();
            String message = String.format("catch exception in elasticSearchDataSource when warm up -> %s, request: %s,  content: %s", e.toString(), request, content);
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
        return sendSyncSearch(esRequest.searchRequest, esRequest.searchContent, esRequest.recallName, null);
    }

    /**
     * poolRecall专用，只返回id，用来历史过滤
     *
     * @param esRequest
     * @return
     */
    @Trace(dispatcher = true)
    public List<String> searchForIds(BaseDataCollection.ESRequest esRequest) {
        if (null == esRequest) {
            return Collections.emptyList();
        }

        List<String> ids = new ArrayList<>();

        try {
            ESRequestCommand command = new ESRequestCommand(restClientThreadLocal, esRequest.searchRequest, esRequest.searchContent);

            if (command.isCircuitBreakerOpen()) {
                NewRelic.noticeError(command.getClass().getSimpleName() + " circuit breaker open!");
            }

            String result = command.execute();
            if (null != result) {
                ids = ESJsonTool.loadOnlyIdList(result);
            }
        } catch (Exception e) {
            e.printStackTrace();
            log.error("catch exception in elasticSearchDataSource -> " + e.toString());
        }
        return ids;
    }

    /**
     * 解析返回结果为shortdocument
     * *
     */
    private List<BaseDocument> loadDocumentOfESSearch(List<JSONObject> resultJSONObjList, String recallName) {
        List<BaseDocument> documentList = new ArrayList<>();
        for (JSONObject jsonObject : resultJSONObjList) {
            ShortDocument doc = new ShortDocument().loadJsonObj(jsonObject, SHORT_VIDEO, recallName);
            if (null != doc) {
                documentList.add(doc);
            }
        }
        return documentList;
    }

    public List<BaseDocument> syncLoadBySearchQyery(String queryBody, String indexAndType, String recallName, IDocumentProcessor processor) {
        List<BaseDocument> resultList = new ArrayList<>();
        String request = "";
        String content = queryBody;
        String requestUrlFormat = "/%s/_search";
        request = String.format(requestUrlFormat, indexAndType);

        for (int tryCount = 0; tryCount <= 2; tryCount++) {
            try {
                resultList = sendSyncSearch(request, content, recallName, processor);
                break;
            } catch (Exception e) {
                if (tryCount >= 2) {
                    log.error("loadDetailInfoForFeed sendPost() Exception for 3 times -> " + e.toString());
                }
            }
        }

        return resultList;
    }

    private void warmup() {
        try {
            String request = String.format("/%s/_search?pretty=false", SHORT_VIDEO.getIndexAndType());
            JSONObject content = new JSONObject();
            JSONObject termsFather = new JSONObject();

            content.put("query", termsFather);
            content.put("size", 1);

            sendSyncPostForWarmUp(request, content.toString());
        } catch (Exception e) {
            e.printStackTrace();
            log.error("warmup failed, just continue...");
            System.out.printf("warmup failed, just continue...");
        }
    }
}
