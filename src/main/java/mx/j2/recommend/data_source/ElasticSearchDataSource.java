package mx.j2.recommend.data_source;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.newrelic.api.agent.NewRelic;
import com.newrelic.api.agent.Trace;
import mx.j2.recommend.conf.Conf;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.hystrix.es.ESRequestCommand;
import mx.j2.recommend.hystrix.es.ScrollRequestCommand;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.ESJsonTool;
import mx.j2.recommend.util.MXJudgeUtils;
import mx.j2.recommend.util.MXStringUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.pool2.impl.AbandonedConfig;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import javax.annotation.concurrent.ThreadSafe;
import java.util.*;


/**
 * ElasticSearch数据服务访问类
 *
 * @author zhangxuejian
 */
@ThreadSafe
public class ElasticSearchDataSource extends BaseDataSource {
    private static Logger log = LogManager.getLogger(ElasticSearchDataSource.class);
    private RestClient restClientThreadLocal;

    private RestHighLevelClient restHighLevelClient;

    public static final List<JSONObject> DEFAULT_RESULTLIST = new ArrayList<>();

    private static final int DEFAULT_ES_CLIENT_NUM = 2000;
    private final static List<BaseDocument> DEFAULT_RECALLLIST = new ArrayList<BaseDocument>();


    /**
     * 构造函数
     *
     * @param
     */
    public ElasticSearchDataSource() {
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
                new HttpHost(Conf.getElasticSearchEndPointURL(), Integer.parseInt(Conf.getElasticSearchEndPointPort()), "http"))
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

        this.restHighLevelClient = new RestHighLevelClient(RestClient.builder(
                new HttpHost(Conf.getElasticSearchEndPointURL(), Integer.parseInt(Conf.getElasticSearchEndPointPort()), "http"))
                .setMaxRetryTimeoutMillis(Conf.getRestClientMaxRetryTimeoutMillis())
                .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultIOReactorConfig(
                        IOReactorConfig.custom()
                                .setIoThreadCount(Conf.getRestClientHttpConfigIoThreadCount())
                                .setSelectInterval(Conf.getRestClientHttpConfigSelectInterval())
                                .setTcpNoDelay(Conf.isRestClientHttpConfigTcpNoDelay())
                                .setSoReuseAddress(Conf.isRestClientHttpConfigSoReuseAddress())
                                .setConnectTimeout(Conf.getRestClientHttpConfigConnectTimeout())
                                .setSoTimeout(Conf.getRestClientHttpConfigSoTimeout())
                                .build()))
                .setRequestConfigCallback(builder -> builder
                        .setConnectionRequestTimeout(Conf.getRestClientRequestConfigConnectionRequestTimeout())
                        .setConnectTimeout(Conf.getRestClientHttpConfigConnectTimeout())
                        .setSocketTimeout(Conf.getRestClientRequestConfigSocketTimeout())));

        warmup();

        log.info("{\"dataSourceInfo\":\"[ElasticSearchSource init successfully]\"}");
    }


    /**
     * 请求ES, 只取结果中的_id信息, 供后续去CA中取详情用
     * *
     */
    @Trace(dispatcher = true)
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

    @Trace(dispatcher = true)
    private Integer loadAccountStatus(String result) {
        JSONObject responseJson = JSONObject.parseObject(result);
        if (responseJson.containsKey("hits")) {
            JSONObject hitObject = responseJson.getJSONObject("hits");
            if (hitObject.containsKey("hits")) {
                JSONArray hitArray = hitObject.getJSONArray("hits");
                for (int i = 0; i < hitArray.size(); i++) {
                    JSONObject obj = hitArray.getJSONObject(i);
                    if (obj.containsKey("_source")) {
                        JSONObject sourceJson = obj.getJSONObject("_source");
                        if (sourceJson.containsKey("status")) {
                            return sourceJson.getInteger("status");
                        }
                    }
                }
            }
        }
        return null;
    }

    @Trace(dispatcher = true)
    private List<BaseDocument> sendSyncSearch(String request, String content, String recallName) {
        List<BaseDocument> objList = new ArrayList<>();

        try {
            ESRequestCommand command = new ESRequestCommand(restClientThreadLocal, request, content);

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

    @Trace(dispatcher = true)
    public List<String> sendSyncSearch(String request, String content) {
        List<String> ids = new ArrayList<>();

        try {
            ESRequestCommand command = new ESRequestCommand(restClientThreadLocal, request, content);

            if (command.isCircuitBreakerOpen()) {
                NewRelic.noticeError(command.getClass().getSimpleName() + " circuit breaker open!");
            }

            String result = command.execute();
            if (null != result) {
                ids = loadOnlyIdList(result);
            }
        } catch (Exception e) {
            ids.clear();
            e.printStackTrace();
            log.error("catch exception in elasticSearchDataSource -> " + e.toString());
        }
        return ids;
    }

    /**
     * 用于可以在初始化时召回的es搜索
     * *
     */
    @Trace(dispatcher = true)
    public List<String> searchForDocumentsScroll(String index, SearchSourceBuilder builder, String recallName, long size) {
        if (null == builder) {
            return Collections.emptyList();
        }
        return sendScrollSearch(index, builder, recallName, size);
    }

    @Trace(dispatcher = true)
    private List<String> sendScrollSearch(String index, SearchSourceBuilder builder, String recallName, long totalSize) {
        List<String> idList = new ArrayList<>();
        try {
            ScrollRequestCommand command = new ScrollRequestCommand(restHighLevelClient, index, builder, totalSize);
            if (command.isCircuitBreakerOpen()) {
                NewRelic.noticeError(command.getClass().getSimpleName() + " circuit breaker open!");
            }
            List<JSONObject> result = command.execute();
            if (CollectionUtils.isNotEmpty(result)) {
                idList = load(result, recallName);
            }
        } catch (Exception e) {
            e.printStackTrace();
            log.error("catch exception in elasticSearchDataSource -> " + e.toString());
        }
        return idList;
    }

    private List<String> load(List<JSONObject> objects, String recallName) {
        List<String> idList = new ArrayList<>();
        String id;
        for (JSONObject jsonObject : objects) {
            if (jsonObject != null) {
                id = jsonObject.getString("metadata_id");
                if (MXJudgeUtils.isNotEmpty(id)) {
                    idList.add(id);
                }
            }
        }
        return idList;
    }

    @Trace(dispatcher = true)
    public List<JSONObject> sendSyncSearchPure(String request, String content) {
        List<JSONObject> objList = new ArrayList<>();

        try {
            ESRequestCommand command = new ESRequestCommand(restClientThreadLocal, request, content);

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
    public Integer sendSyncSearchForAccountStatus(String request, String content) {
        Integer status = 0;
        try {
            ESRequestCommand command = new ESRequestCommand(restClientThreadLocal, request, content);

            if (command.isCircuitBreakerOpen()) {
                NewRelic.noticeError(command.getClass().getSimpleName() + " circuit breaker open!");
            }

            String result = command.execute();

            if (MXStringUtils.isNotEmpty(result)) {
                status = loadAccountStatus(result);
            }
        } catch (Exception e) {
            status = 0;
            e.printStackTrace();
            log.error("catch exception in elasticSearchDataSource -> " + e.toString());
        }
        return status;
    }

    @Trace(dispatcher = true)
    public List<JSONObject> sendSyncSearchforCard(String request, String content) {
        List<JSONObject> objList = new ArrayList<>();

        try {
            ESRequestCommand command = new ESRequestCommand(restClientThreadLocal, request, content);

            if (command.isCircuitBreakerOpen()) {
                NewRelic.noticeError(command.getClass().getSimpleName() + " circuit breaker open!");
            }

            String result = command.execute();

            if (null != result) {
                objList = ESJsonTool.loadCardList(result);
            }
        } catch (Exception e) {
            objList.clear();
            e.printStackTrace();
            log.error("catch exception in elasticSearchDataSource -> " + e.toString());
        }
        return objList;
    }

    @Trace(dispatcher = true)
    public List<JSONObject> sendSyncSearchforLive(String request, String content) {
        List<JSONObject> objList = new ArrayList<>();

        try {
            ESRequestCommand command = new ESRequestCommand(restClientThreadLocal, request, content);

            if (command.isCircuitBreakerOpen()) {
                NewRelic.noticeError(command.getClass().getSimpleName() + " circuit breaker open!");
            }

            String result = command.execute();

            if (null != result) {
                objList = ESJsonTool.loadLiveList(result);
            }
        } catch (Exception e) {
            objList.clear();
            e.printStackTrace();
            log.error("catch exception in elasticSearchDataSource -> " + e.toString());
        }
        return objList;
    }

    @Trace(dispatcher = true)
    public List<JSONObject> syncLoadDetailBySearch(int size, List<String> idList, String indexAndType) {
        List<JSONObject> resultList = new ArrayList<>();
        if (MXJudgeUtils.isEmpty(idList)) {
            return resultList;
        }
        String request = "";
        String content = "";
        if (MXStringUtils.isNotEmpty(indexAndType)) {
            String requestUrlFormat = "/%s/_search";
            request = String.format(requestUrlFormat, indexAndType);
        } else {
            request = "/taka_flowpool_lv*/_search";
        }

        JSONObject contentJson = new JSONObject();
        JSONObject termsFather = new JSONObject();
        JSONObject terms = new JSONObject();
        terms.put("_id", idList);
        termsFather.put("terms", terms);
        contentJson.put("query", termsFather);
        contentJson.put("size", size);
        content = contentJson.toString();

        for (int tryCount = 0; tryCount <= 2; tryCount++) {
            try {
                ESRequestCommand command = new ESRequestCommand(restClientThreadLocal, request, content);

                if (command.isCircuitBreakerOpen()) {
                    NewRelic.noticeError(command.getClass().getSimpleName() + " circuit breaker open!");
                }

                String result = command.execute();
                if (null != result) {
                    resultList = ESJsonTool.loadPoolIndexList(result);
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


    @Trace(dispatcher = true)
    private void sendSyncPostForWarmUp(String request, String content) {
        try {
            ESRequestCommand command = new ESRequestCommand(restClientThreadLocal, request, content);

            if (command.isCircuitBreakerOpen()) {
                NewRelic.noticeError(command.getClass().getSimpleName() + " circuit breaker open!");
            }

            String result = command.execute();

            if (null != result) {
                loadOnlyIdList(result);
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
        return sendSyncSearch(esRequest.searchRequest, esRequest.searchContent, esRequest.recallName);
    }

    /**
     * poolRecall专用，只返回id，用来历史过滤
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
                ids = loadOnlyIdList(result);
            }
        } catch (Exception e) {
            e.printStackTrace();
            log.error("catch exception in elasticSearchDataSource -> " + e.toString());
        }
        return ids;
    }

    private void warmup() {
        try {
            String request = String.format("/%s/_search?pretty=false", "taka_flowpool_lv1");
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

    @Trace(dispatcher = true)
    public Map<String, Long> sendSyncReturnPublisherAndTime(String request, String content) {
        Map<String, Long> map = new HashMap<>();
        try {
            ESRequestCommand command = new ESRequestCommand(restClientThreadLocal, request, content);
            if (command.isCircuitBreakerOpen()) {
                NewRelic.noticeError(command.getClass().getSimpleName() + " circuit breaker open!");
            }
            String result = command.execute();
            if (null != result) {
                JSONObject responseJson = JSONObject.parseObject(result);
                if (responseJson.containsKey("hits")) {
                    JSONObject hitObject = responseJson.getJSONObject("hits");
                    if (hitObject.containsKey("hits")) {
                        JSONArray hitArray = hitObject.getJSONArray("hits");
                        for (int i = 0; i < hitArray.size(); i++) {
                            JSONObject obj = hitArray.getJSONObject(i);
                            if (obj.containsKey("_source")) {
                                JSONObject source = obj.getJSONObject("_source");
                                if (!source.containsKey("publisher_id") || !source.containsKey("start_time")) {
                                    continue;
                                }
                                map.put(source.getString("publisher_id"), source.getLongValue("start_time"));
                            }
                        }
                    }
                }
            }
            return map;
        } catch (Exception e) {
            e.printStackTrace();
            log.error("catch exception in elasticSearchDataSource -> " + e.toString());
        }
        return Collections.emptyMap();
    }

}
