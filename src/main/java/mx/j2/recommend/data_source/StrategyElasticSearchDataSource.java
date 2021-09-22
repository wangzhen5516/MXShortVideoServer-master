package mx.j2.recommend.data_source;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.newrelic.api.agent.NewRelic;
import com.newrelic.api.agent.Trace;
import mx.j2.recommend.conf.Conf;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_model.document.ShortDocument;
import mx.j2.recommend.hystrix.es.StrategyESRequestCommand;
import mx.j2.recommend.hystrix.es.StrategyESRequestCommandAsync;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.manager.MXManager;
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

import static mx.j2.recommend.util.DefineTool.CategoryEnum.DENSE_VECTOR_VIDEO;
import static mx.j2.recommend.util.DefineTool.CategoryEnum.SHORT_VIDEO;
import static mx.j2.recommend.util.DefineTool.FlowInterface.*;


/**
 * ElasticSearch数据服务访问类
 *
 * @author zhongren.li
 */
@ThreadSafe
public class StrategyElasticSearchDataSource extends BaseDataSource {
    private static Logger log = LogManager.getLogger(StrategyElasticSearchDataSource.class);
    private RestClient restClientThreadLocal;

    private static final int DEFAULT_ES_CLIENT_NUM = 2000;
    private final static List<BaseDocument> DEFAULT_RECALLLIST = new ArrayList<BaseDocument>();

    /**
     * 构造函数
     */
    public StrategyElasticSearchDataSource() {
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
                new HttpHost(Conf.getStrategyElasticSearchEndPointURL(), Integer.parseInt(Conf.getStrategyElasticSearchEndPointPort()), "http"))
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

        log.info("{\"dataSourceInfo\":\"[StrategyElasticSearchDataSource init successfully]\"}");
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
        dc.asynRecallList.addAll(searchEngineRecallList);
        dc.moduleEndTime = System.nanoTime();
        dc.appendToTimeRecord(dc.moduleEndTime - dc.moduleStartTime, "ElasticSearch");
    }

    private List<BaseDocument> getDetail(BaseDataCollection dc, List<String> ids) {
        List<BaseDocument> objects = MXDataSource.details().get(ids, "");

        /**
         *  如果是这两个接口, 时间要以ES为准, 否则会有排序问题, 其他接口暂时不用
         */
        if (MX_VIDEOS_OF_THE_PUBLISHER_VERSION_1_0.getName().equals(dc.req.interfaceName) ||
                MX_VIDEOS_OF_THE_PUBLISHER_ME_VERSION_1_0.getName().equals(dc.req.interfaceName) ||
                MX_VIDEOS_OF_THE_SAME_AUDIO_VERSION_1_0.getName().equals(dc.req.interfaceName) ||
                MX_VIDEOS_OF_THE_TAG_VERSION_1_0.getName().equals(dc.req.interfaceName)) {
            //log.error(onlineTimeMap);
            // online_time 以ES的返回结果为准
            for (BaseDocument object : objects) {
                String id = object.getId();
                object.setRecallName(dc.idToRecallNameMap.get(id));
            }
        }

        return objects;
    }

    /**
     * 发送请求
     *
     * @param dc dc
     */
    @Trace(dispatcher = true)
    private List<String> sendAsyncPost(BaseDataCollection dc) {
        final List<String> idsFromElasticSearch = Collections.synchronizedList(new ArrayList<>());

        List<BaseDataCollection.ESRequest> reqList = dc.strategySearchRequestList;
        if (null == reqList) {
            return idsFromElasticSearch;
        }

        final CountDownLatch latch = new CountDownLatch(reqList.size());
        try {
            for (BaseDataCollection.ESRequest request : reqList) {
                final BaseDataCollection.ESRequest finalRequest = request;

                StrategyESRequestCommandAsync commandAsync = new StrategyESRequestCommandAsync(
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
                                    List<String> ids = loadOnlyIdList(result);
                                    idsFromElasticSearch.addAll(ids);
                                    ids.forEach(id -> {
                                        dc.idToRecallNameMap.put(id, finalRequest.recallName);
                                    });
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
            log.error("catch exception in StrategyElasticSearchDataSource -> " + e.toString());
            NewRelic.noticeError("catch exception in StrategyElasticSearchDataSource -> " + e.toString());
            String reqStr = "";
            for (BaseDataCollection.ESRequest request : reqList) {
                reqStr += request.searchRequest + ":" + request.searchContent + ";";
            }
            String message = String.format("catch exception in StrategyElasticSearchDataSource -> %s, req -> %s", e.toString(), reqStr);
            LogTool.printErrorLog(log, DefineTool.ErrorNoEnum.NORMAL_ERROR.getErrorNo(), message, null, null);
        }
        return idsFromElasticSearch;
    }


    /**
     * 请求ES, 只取结果中的_id信息, 供后续去CA中取详情用
     * *
     */
    private List<String> loadOnlyIdList(String result) {
        List<String> idList = new ArrayList<>();
        JSONObject responseJson = JSONObject.parseObject(result);

        if (responseJson.containsKey("hits")) {
            JSONObject hitObject = responseJson.getJSONObject("hits");
            if (hitObject.containsKey("hits")) {
                JSONArray hitArray = hitObject.getJSONArray("hits");
                for (int i = 0; i < hitArray.size(); i++) {
                    JSONObject obj = hitArray.getJSONObject(i);
                    if (!obj.containsKey("_id")) {
                        continue;
                    }
                    idList.add(obj.getString("_id"));
                }
            }
        }
        return idList;
    }

    /**
     * 对请求结果进行解析，将视频id和距离信息保存在map中
     *
     * @param result
     * @return
     */
    private JSONObject getHeatScoreAndDistance(String result) {
        JSONObject heatScoreAndDistanceJSON = new JSONObject(true);

        JSONObject responseJson = JSONObject.parseObject(result);
        if (responseJson.containsKey("hits")) {
            JSONObject hitObject = responseJson.getJSONObject("hits");
            if (hitObject.containsKey("hits")) {
                JSONArray hitArray = hitObject.getJSONArray("hits");
                for (int i = 0; i < hitArray.size(); i++) {
                    JSONObject obj = hitArray.getJSONObject(i);
                    if (!obj.containsKey("_id") || !obj.containsKey("sort") || obj.getJSONArray("sort").size() < 4 || !obj.containsKey("_source") || obj.getJSONObject("_source") == null) {
                        continue;
                    }
                    JSONObject heatScoreAndDistance = new JSONObject();
                    heatScoreAndDistance.put("onlineTimeNeed", obj.getJSONArray("sort").getLongValue(0));
                    heatScoreAndDistance.put("heatScore", obj.getJSONArray("sort").getDoubleValue(1));
                    heatScoreAndDistance.put("distance", obj.getJSONArray("sort").getDoubleValue(2));
                    heatScoreAndDistanceJSON.put(obj.getString("_id"), heatScoreAndDistance);
                }
            }
        }
        return heatScoreAndDistanceJSON;
    }

    private List<JSONObject> loadVector(String result) {
        List<JSONObject> objectList = new ArrayList<>();
        JSONObject responseJson = JSONObject.parseObject(result);

        if (responseJson.containsKey("hits")) {
            JSONObject hitObject = responseJson.getJSONObject("hits");
            if (hitObject.containsKey("hits")) {
                JSONArray hitArray = hitObject.getJSONArray("hits");
                for (int i = 0; i < hitArray.size(); i++) {
                    JSONObject object = hitArray.getJSONObject(i);
                    if (object.containsKey("_source")) {
                        objectList.add(object.getJSONObject("_source"));
                    }
                }
            }
        }
        return objectList;
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

    public List<JSONObject> syncLoadDetailBySearch(String key, int size, List<String> idList, String indexAndType, List<String> sourceList) {
        List<JSONObject> resultList = new ArrayList<>();
        if (MXJudgeUtils.isEmpty(idList)) {
            return resultList;
        }
        String request = "";
        String content = "";
        String requestUrlFormat = "/%s/_search";
        request = String.format(requestUrlFormat, indexAndType);

        String termsKey = "_id";
        if (MXJudgeUtils.isNotEmpty(key)) {
            termsKey = key;
        }
        content = constructSearchContent(termsKey, size, idList, sourceList);

        for (int tryCount = 0; tryCount <= 2; tryCount++) {
            try {
                // 本方法没有被使用，此句先注释掉
                //resultList = sendSyncSearch(request, content);
                break;
            } catch (Exception e) {
                if (tryCount >= 2) {
                    log.error("loadDetailInfoForFeed sendPost() Exception for 3 times -> " + e.toString());
                }
            }
        }

        return resultList;
    }

    public List<JSONObject> syncLoadDetailBySearch(int size, List<String> idList, String indexAndType, List<String> sourceList) {
        List<JSONObject> resultList = new ArrayList<>();
        if (MXJudgeUtils.isEmpty(idList)) {
            return resultList;
        }
        String request = "";
        String content = "";
        String requestUrlFormat = "/%s/_search";
        request = String.format(requestUrlFormat, indexAndType);

        String termsKey = "_id";
        content = constructSearchContent(termsKey, size, idList, sourceList);

        for (int tryCount = 0; tryCount <= 2; tryCount++) {
            try {
                StrategyESRequestCommand command = new StrategyESRequestCommand(restClientThreadLocal, request, content);

                if (command.isCircuitBreakerOpen()) {
                    NewRelic.noticeError(command.getClass().getSimpleName() + " circuit breaker open!");
                }

                String result = command.execute();
                if (null != result) {
                    resultList = loadVector(result);
                }
                break;
            } catch (Exception e) {
                if (tryCount >= 2) {
                    log.error("loadDetailInfoForFeed sendPost() Exception for 3 times -> " + e.toString());
                }
            }
        }

        return resultList;
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
            StrategyESRequestCommand command = new StrategyESRequestCommand(restClientThreadLocal, request, content);

            if (command.isCircuitBreakerOpen()) {
                NewRelic.noticeError(command.getClass().getSimpleName() + " circuit breaker open!");
            }

            String result = command.execute();

            if (null != result) {
                JSONObject responseJson = JSONObject.parseObject(result);
                if (responseJson.containsKey("hits")) {
                    JSONObject hitObject = responseJson.getJSONObject("hits");
                    if (hitObject.containsKey("total")) {
                        retNum = hitObject.getInteger("total");
                    }
                }
            }
            return retNum;
        } catch (Exception e) {
            e.printStackTrace();
            log.error("catch exception in StrategyElasticSearchDataSource -> " + e.toString());
        }
        return 0;
    }

    private List<BaseDocument> sendSyncSearch(String request, String content, String recallName) {
        List<BaseDocument> objList = new ArrayList<>();

        try {
            StrategyESRequestCommand command = new StrategyESRequestCommand(restClientThreadLocal, request, content);

            if (command.isCircuitBreakerOpen()) {
                NewRelic.noticeError(command.getClass().getSimpleName() + " circuit breaker open!");
            }

            String result = command.execute();

            if (null != result) {
                List<String> ids = loadOnlyIdList(result);
                objList = MXDataSource.details().get(ids, recallName);
            }
        } catch (Exception e) {
            objList.clear();
            e.printStackTrace();
            log.error("catch exception in elasticSearchDataSource -> " + e.toString());
        }
        return objList;
    }

    /**
     * 向es发送请求
     *
     * @param request
     * @param content
     * @return
     */
    private JSONObject sendSyncSearchToGetHeatScoreAndDistance(String request, String content) {
        try {
            StrategyESRequestCommand command = new StrategyESRequestCommand(restClientThreadLocal, request, content);

            if (command.isCircuitBreakerOpen()) {
                NewRelic.noticeError(command.getClass().getSimpleName() + " circuit breaker open!");
            }

            String result = command.execute();

            if (MXStringUtils.isNotEmpty(result)) {
                Map<String, String> nextTokenMap = ESJsonTool.loadIdSortMap(result);
                JSONObject res = new JSONObject();
                res.put("tokenMap", nextTokenMap);
                res.put("heatScore", getHeatScoreAndDistance(result));
                return res;
            }
        } catch (Exception e) {
            e.printStackTrace();
            log.error("catch exception in elasticSearchDataSource -> " + e.toString());
        }
        return new JSONObject();
    }


    @Trace(dispatcher = true)
    private void sendSyncPostForWarmUp(String request, String content) {
        try {
            StrategyESRequestCommand command = new StrategyESRequestCommand(restClientThreadLocal, request, content);

            if (command.isCircuitBreakerOpen()) {
                NewRelic.noticeError(command.getClass().getSimpleName() + " circuit breaker open!");
            }

            String result = command.execute();

            if (null != result) {
                loadOnlyIdList(result);
            }
        } catch (Exception e) {
            e.printStackTrace();
            String message = String.format("catch exception in StrategyElasticSearchDataSource when warm up -> %s, request: %s,  content: %s", e.toString(), request, content);
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
        // 此方法没有被使用，此句先注释掉
        List<JSONObject> resultJSONObjList = null;//sendSyncSearch(esRequest.searchRequest, esRequest.searchContent);
        return loadDocumentOfESSearch(resultJSONObjList, esRequest.recallName);
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

    public List<BaseDocument> syncLoadBySearchQyery(String queryBody, String indexAndType, String recallName) {
        List<BaseDocument> resultList = new ArrayList<>();
        String request = "";
        String content = queryBody;
        String requestUrlFormat = "/%s/_search";
        request = String.format(requestUrlFormat, indexAndType);

        for (int tryCount = 0; tryCount <= 2; tryCount++) {
            try {
                resultList = sendSyncSearch(request, content, recallName);
                break;
            } catch (Exception e) {
                if (tryCount >= 2) {
                    log.error("loadDetailInfoForFeed sendPost() Exception for 3 times -> " + e.toString());
                }
            }
        }

        return resultList;
    }

    /**
     * 从es中获取视频id和距离信息
     *
     * @param queryBody
     * @param indexAndType
     * @return
     */
    public JSONObject getHeatScoreAndDistanceFromES(String queryBody, String indexAndType) {
        JSONObject heatScoreAndDistanceJson = new JSONObject();
        String request = "";
        String content = queryBody;
        String requestUrlFormat = "/%s/_search";
        request = String.format(requestUrlFormat, indexAndType);

        for (int tryCount = 0; tryCount <= 2; tryCount++) {
            try {
                heatScoreAndDistanceJson = sendSyncSearchToGetHeatScoreAndDistance(request, content);
                break;
            } catch (Exception e) {
                if (tryCount >= 2) {
                    log.error("loadDetailInfoForFeed sendPost() Exception for 3 times -> " + e.toString());
                }
            }
        }
        return heatScoreAndDistanceJson;
    }

    private void warmup() {
        try {
            String request = String.format("/%s/_search?pretty=false", DENSE_VECTOR_VIDEO.getIndexAndType());
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
