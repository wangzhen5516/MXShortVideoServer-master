package mx.j2.recommend.data_source;

import com.alibaba.fastjson.JSONObject;
import com.alicp.jetcache.Cache;
import com.alicp.jetcache.redis.lettuce.RedisLettuceCacheBuilder;
import com.alicp.jetcache.support.FastjsonKeyConvertor;
import com.alicp.jetcache.support.JavaValueDecoder;
import com.alicp.jetcache.support.JavaValueEncoder;
import com.netflix.hystrix.HystrixCommand;
import com.newrelic.api.agent.NewRelic;
import com.newrelic.api.agent.Trace;
import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import mx.j2.recommend.conf.Conf;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.SageMakerPublisherFeatureDocument;
import mx.j2.recommend.hystrix.*;
import mx.j2.recommend.hystrix.redis.*;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.thrift.Result;
import mx.j2.recommend.util.*;
import org.apache.commons.collections.MapUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.concurrent.ThreadSafe;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


/**
 * 亚马逊ElasticCache 服务接口
 *
 * @author zhuowei
 */
@ThreadSafe
public class ElasticCacheSource extends BaseDataSource {

    private static Logger logger = LogManager.getLogger(ElasticCacheSource.class);

    /**
     * 业务Redis相关
     */
    public RedisURI cacheRedisUri = null;
    public RedisClusterClient cacheClusterClient = null;
    public StatefulRedisClusterConnection<String, String> cacheClusterConnection = null;

    /**
     * 业务Redis相关
     */
    public RedisURI cacheNewRedisUri = null;
    public RedisClusterClient cacheNewClusterClient = null;
    public StatefulRedisClusterConnection<String, String> cacheNewClusterConnection = null;

    /**
     * TopHot历史redis
     */
    public RedisURI cacheTopHotRedisUri = null;
    public RedisClusterClient cacheTopHotClusterClient = null;
    public StatefulRedisClusterConnection<String, String> cacheTopHotClusterConnection = null;

    /**
     * 策略Redis相关
     */
    public RedisURI strategyRedisUri = null;
    public RedisClusterClient strategyClusterClient = null;
    public StatefulRedisClusterConnection<String, String> strategyClusterConnection = null;

    /**
     * 隐私账户存储
     */
    public RedisURI privateAccountRedisUri = null;
    public RedisClusterClient privateAccountClusterClient = null;
    public StatefulRedisClusterConnection<String, String> privateAccountClusterConnection = null;


    private RedisURI pubFeatureRedisUri = null;
    private RedisClusterClient pubFeatureClusterClient = null;
    private StatefulRedisClusterConnection<String, String> pubFeatureClusterConnection = null;
    /**
     * lettuce cache相关
     */
    private Cache<String, List<Result>> userResultCache;

    /**
     * 构造函数
     */
    public ElasticCacheSource() {
        init();
    }

    /**
     * 初始化
     */
    public void init() {
        cacheRedisUri = RedisURI.Builder.redis(Conf.getRedisCacheHost(), 6379)
                .withTimeout(Duration.ofMillis(Conf.getJedisClusterSocketTimeout()))
                .build();
        cacheClusterClient = RedisClusterClient.create(cacheRedisUri);
        cacheClusterClient.setOptions(ClusterClientOptions.builder()
                .validateClusterNodeMembership(false)
                .build());
        cacheClusterConnection = cacheClusterClient.connect();

        cacheNewRedisUri = RedisURI.Builder.redis(Conf.getRedisCacheNewHost(), 6379)
                .withTimeout(Duration.ofMillis(Conf.getJedisClusterSocketTimeout()))
                .build();
        cacheNewClusterClient = RedisClusterClient.create(cacheNewRedisUri);
        cacheNewClusterClient.setOptions(ClusterClientOptions.builder()
                .validateClusterNodeMembership(false)
                .build());
        cacheNewClusterConnection = cacheNewClusterClient.connect();

        cacheTopHotRedisUri = RedisURI.Builder.redis(Conf.getRedisCacheTopHotHost(), 6379)
                .withTimeout(Duration.ofMillis(Conf.getJedisClusterSocketTimeout()))
                .build();
        cacheTopHotClusterClient = RedisClusterClient.create(cacheTopHotRedisUri);
        cacheTopHotClusterClient.setOptions(ClusterClientOptions.builder()
                .validateClusterNodeMembership(false)
                .build());
        cacheTopHotClusterConnection = cacheTopHotClusterClient.connect();

        strategyRedisUri = RedisURI.Builder.redis(Conf.getRedisStrategyHost(), Conf.getRedisStrategyPort())
                .withTimeout(Duration.ofMillis(Conf.getJedisClusterSocketTimeout()))
                .build();
        strategyClusterClient = RedisClusterClient.create(strategyRedisUri);
        strategyClusterClient.setOptions(ClusterClientOptions.builder()
                .validateClusterNodeMembership(false)
                .build());
        strategyClusterConnection = strategyClusterClient.connect();

        privateAccountRedisUri = RedisURI.Builder.redis(Conf.getRedisPrivateAccountHost(), 6379)
                .withTimeout(Duration.ofMillis(Conf.getJedisClusterSocketTimeout()))
                .build();
        privateAccountClusterClient = RedisClusterClient.create(privateAccountRedisUri);
        privateAccountClusterClient.setOptions(ClusterClientOptions.builder()
                .validateClusterNodeMembership(false)
                .build());
        privateAccountClusterConnection = privateAccountClusterClient.connect();

        pubFeatureRedisUri = RedisURI.Builder.redis(Conf.getPubFeatureRedisHost(), Conf.getPubFeatureRedisPort())
                .withTimeout(Duration.ofMillis(Conf.getJedisClusterSocketTimeout()))
                .build();
        pubFeatureClusterClient = RedisClusterClient.create(pubFeatureRedisUri);
        pubFeatureClusterClient.setOptions(ClusterClientOptions.builder()
                .validateClusterNodeMembership(false)
                .build());
        pubFeatureClusterConnection = pubFeatureClusterClient.connect();

        // lettuce cache init
        userResultCache = RedisLettuceCacheBuilder.createRedisLettuceCacheBuilder()
                .keyConvertor(FastjsonKeyConvertor.INSTANCE)
                .valueDecoder(JavaValueDecoder.INSTANCE)
                .valueEncoder(JavaValueEncoder.INSTANCE)
                .redisClient(cacheClusterClient)
                .keyPrefix("urc_")
                .buildCache();

        logger.info("{\"dataSourceInfo\":\"[ElasticCacheSource init successfully]\"}");
    }

    @Trace(dispatcher = true)
    public StatefulRedisClusterConnection<String, String> getCacheConnection() {
        if (null == cacheClusterConnection || !cacheClusterConnection.isOpen()) {
            if (null == cacheClusterClient) {
                cacheClusterClient = RedisClusterClient.create(cacheRedisUri);
                cacheClusterConnection = cacheClusterClient.connect();
            } else {
                cacheClusterConnection = cacheClusterClient.connect();
            }
        }
        return cacheClusterConnection;
    }

    @Trace(dispatcher = true)
    public StatefulRedisClusterConnection<String, String> getCacheNewConnection() {
        if (null == cacheNewClusterConnection || !cacheNewClusterConnection.isOpen()) {
            if (null == cacheNewClusterClient) {
                cacheNewClusterClient = RedisClusterClient.create(cacheNewRedisUri);
                cacheNewClusterConnection = cacheNewClusterClient.connect();
            } else {
                cacheNewClusterConnection = cacheNewClusterClient.connect();
            }
        }
        return cacheNewClusterConnection;
    }

    @Trace(dispatcher = true)
    public StatefulRedisClusterConnection<String, String> getCacheTopHotConnection() {
        if (null == cacheTopHotClusterConnection || !cacheTopHotClusterConnection.isOpen()) {
            if (null == cacheTopHotClusterClient) {
                cacheTopHotClusterClient = RedisClusterClient.create(cacheTopHotRedisUri);
                cacheTopHotClusterConnection = cacheTopHotClusterClient.connect();
            } else {
                cacheTopHotClusterConnection = cacheTopHotClusterClient.connect();
            }
        }
        return cacheTopHotClusterConnection;
    }

    @Trace(dispatcher = true)
    public StatefulRedisClusterConnection<String, String> getStrategyConnection() {
        if (null == strategyClusterConnection || !strategyClusterConnection.isOpen()) {
            if (null == strategyClusterClient) {
                strategyClusterClient = RedisClusterClient.create(strategyRedisUri);
                strategyClusterConnection = strategyClusterClient.connect();
            } else {
                strategyClusterConnection = strategyClusterClient.connect();
            }
        }
        return strategyClusterConnection;
    }

    @Trace(dispatcher = true)
    public StatefulRedisClusterConnection<String, String> getPrivateAccountClusterConnection() {
        if (null == privateAccountClusterConnection || !privateAccountClusterConnection.isOpen()) {
            if (null == privateAccountClusterClient) {
                privateAccountClusterClient = RedisClusterClient.create(privateAccountRedisUri);
                privateAccountClusterConnection = privateAccountClusterClient.connect();
            } else {
                privateAccountClusterConnection = privateAccountClusterClient.connect();
            }
        }
        return privateAccountClusterConnection;
    }

    public StatefulRedisClusterConnection<String, String> getPubFeatureClusterConnection() {
        if (null == pubFeatureClusterConnection || !pubFeatureClusterConnection.isOpen()) {
            if (null == pubFeatureClusterClient) {
                pubFeatureClusterClient = RedisClusterClient.create(pubFeatureRedisUri);
                pubFeatureClusterConnection = pubFeatureClusterClient.connect();
            } else {
                pubFeatureClusterConnection = pubFeatureClusterClient.connect();
            }
        }
        return pubFeatureClusterConnection;
    }

    /**
     * 从redis中获取Uuid绑定的flow
     *
     * @param dc
     * @return
     */
    public String getFlowByUuid(BaseDataCollection dc) {
        try {
            HystrixCommand<String> command = new GetUserFlowCommand(dc);
            if (command.isCircuitBreakerOpen()) {
                NewRelic.noticeError(command.getCommandKey().name() + " circuit breaker open!");
            }
            return command.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 从redis中取得用户的推荐历史列表
     */
    @Trace(dispatcher = true)
    public boolean getUserRecommendHistoryList(BaseDataCollection dc) {
        try {
            HystrixCommand<Boolean> command = new GetUserHistoryCommand(dc);
            if (command.isCircuitBreakerOpen()) {
                NewRelic.noticeError(command.getCommandKey().name() + " circuit breaker open!");
            }
            return command.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    @Trace(dispatcher = true)
    public boolean getUserRecommendHistoryListNew(BaseDataCollection dc) {
        try {
            HystrixCommand<Boolean> command = new GetUserHistoryNewCommand(dc);
            if (command.isCircuitBreakerOpen()) {
                NewRelic.noticeError(command.getCommandKey().name() + " circuit breaker open!");
            }
            return command.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    @Trace(dispatcher = true)
    public boolean getUserRecommendHistoryListOnlyTopHot(BaseDataCollection dc) {
        try {
            HystrixCommand<Boolean> command = new GetUserHistoryOnlyTopHotCommand(dc);
            if (command.isCircuitBreakerOpen()) {
                NewRelic.noticeError(command.getCommandKey().name() + " circuit breaker open!");
            }
            return command.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    @Trace(dispatcher = true)
    public boolean getUserRecommendHistoryListNotTopHot(BaseDataCollection dc) {
        try {
            HystrixCommand<Boolean> command = new GetUserHistoryNotTopHotCommand(dc);
            if (command.isCircuitBreakerOpen()) {
                NewRelic.noticeError(command.getCommandKey().name() + " circuit breaker open!");
            }
            return command.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }


    public void getUserLastRecommendHistoryId(BaseDataCollection dc) {
        try {
            HystrixCommand<Boolean> command = new GetUserLastHistoryIdCommand(dc);
            if (command.isCircuitBreakerOpen()) {
                NewRelic.noticeError(command.getCommandKey().name() + " circuit breaker open!");
            }
            command.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 向redis中设置用户的推荐历史列表
     */
    @Trace(dispatcher = true)
    public boolean setUserRecommendHistoryList(BaseDataCollection dc) {
        if (dc.resIdList.isEmpty()) {
            return true;
        }

        boolean isVipUser = false;
//        if (dc.isDebugModeOpen) {
//            isVipUser = true;
//        }
        try {
            if (dc.client.user.isHaveMachineID) {
                long historySize = dc.userHistorySize;
                HystrixCommand<Void> command = new SetUserHistoryCommand(dc.client.user.adId + Conf.getHistoryIdsSuffix(), dc.resIdList, historySize, isVipUser);
                command.execute();
            }

            HystrixCommand<Integer> command2 = new SetUserHistoryOnlyTopHotCommand(dc.client.user.uuId, new HashSet<String>(MXCollectionUtils.except(dc.resIdList, dc.topHotIdList)), isVipUser);
            command2.execute();
        } catch (Exception e) {
            e.printStackTrace();
            NewRelic.noticeError("setUserRecommendHistoryList Exception");
            return false;
        }
        return true;
    }

    /**
     * 向redis中设置用户的推荐历史列表
     */
    @Trace(dispatcher = true)
    public boolean setUserRecommendHistoryListNew(BaseDataCollection dc) {
        if (dc.resIdList.isEmpty()) {
            return true;
        }
        boolean isVipUser = false;
//        if (dc.isDebugModeOpen) {
//            isVipUser = true;
//        } else
        if (dc.client.user.uuId.equals(dc.client.user.userId)) {//过滤掉非登录用户，后面的uuId已经可以存储了
            return true;
        }
        HystrixCommand<Integer> command = new SetUserHistoryNewCommand(dc.client.user.userId, dc.resIdList, isVipUser);
        Integer ret = command.execute();
        if (ret != null && ret >= 0) {
            return true;
        }
        return false;
    }

    public boolean setUserRecommendHistoryListOnlyTopHot(BaseDataCollection dc) {
        if (dc.topHotIdList.isEmpty()) {
            return true;
        }

        boolean isVipUser = false;
//        if (dc.isDebugModeOpen) {
//            isVipUser = true;
//        }
        HystrixCommand<Integer> command = new SetUserHistoryOnlyTopHotCommand(dc.client.user.uuId, dc.topHotIdList, isVipUser);
        Integer ret = command.execute();
        if (ret != null && ret >= 0) {
            return true;
        }
        return false;
    }

    public boolean setUserRecommendHistoryListNotTopHot(BaseDataCollection dc) {
        if (dc.notTopHotIdList.isEmpty()) {
            return true;
        }
        boolean isVipUser = false;
//        if (dc.isDebugModeOpen) {
//            isVipUser = true;
//        }

        try {
            HystrixCommand<Void> command = new SetUserHistoryNotTopHotCommand(dc.client.user.uuId, dc.notTopHotIdList, isVipUser);
            command.execute();
        } catch (Exception e) {
            e.printStackTrace();
            NewRelic.noticeError("setUserRecommendHistoryListNotTopHot Exception");
            return false;
        }
        return true;
    }

    /**
     * 从缓存中取出Result list
     */
    @Trace(dispatcher = true)
    public Map<String, Double> getManualControltCache(String keyStr, long end) {
        try {
            HystrixCommand<Map<String, Double>> command = new ZrevRangeWithScoresStragegyCommand(keyStr, end);
            if (command.isCircuitBreakerOpen()) {
                NewRelic.noticeError(command.getCommandKey().name() + " circuit breaker open!");
            }
            return command.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return Collections.emptyMap();
    }

    @Trace(dispatcher = true)
    public List<String> getZrevRangeStrageyList(String keyStr, long start, long end) {
        try {
            HystrixCommand<List<String>> command = new ZrevRangeStragegyCommand(keyStr, start, end);
            if (command.isCircuitBreakerOpen()) {
                NewRelic.noticeError(command.getCommandKey().name() + " circuit breaker open!");
            }
            return command.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return Collections.emptyList();
    }

    @Trace(dispatcher = true)
    public List<String> getTopKOLPublisherIds(String key, long start, long end) {
        List<String> publisherIds;
        try {
            ZrevRangeStrategyCommand publisherIdCommand = new ZrevRangeStrategyCommand(key, start, end);
            publisherIds = publisherIdCommand.execute();
        } catch (Exception e) {
            e.printStackTrace();
            return Collections.EMPTY_LIST;
        }
        return publisherIds;
    }

    @Trace(dispatcher = true)
    public Set<String> getPrivateAccountSet(String keyStr) {
        try {
            HystrixCommand<Set<String>> command = new PrivateAccountSetCommand(keyStr);
            if (command.isCircuitBreakerOpen()) {
                NewRelic.noticeError(command.getCommandKey().name() + " circuit breaker open!");
            }
            return command.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return Collections.emptySet();
    }

    /**
     * 从 redis 中取出敏感词
     *
     * @param keyStr redis Key
     * @return 敏感词set
     */
    @Trace(dispatcher = true)
    public Set<String> getSensitiveWordsList(String keyStr) {
        try {
            HystrixCommand<Set<String>> command = new SensitiveWordsListCommand(keyStr);
            if (command.isCircuitBreakerOpen()) {
                NewRelic.noticeError(command.getCommandKey().name() + " circuit breaker open!");
            }
            return command.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return Collections.emptySet();
    }

    @Trace(dispatcher = true)
    public Map<String, String> getBindAudioMap(String key) {
        try {
            HystrixCommand<Map<String, String>> command = new HGetAllBindAudioCommand(key);
            if (command.isCircuitBreakerOpen()) {
                NewRelic.noticeError(command.getCommandKey().name() + " circuit breaker open!");
            }
            return command.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return Collections.emptyMap();
    }

    /**
     * 缓存Result list
     *
     * @param dc
     */
    @Trace(dispatcher = true)
    public void setResultCache(BaseDataCollection dc) {
        if (MXJudgeUtils.isEmpty(dc.cachedResultList)) {
            return;
        }
        try {
            HystrixCommand<Void> command = new SetUserResultListCommand(dc);
            if (command.isCircuitBreakerOpen()) {
                NewRelic.noticeError(command.getCommandKey().name() + " circuit breaker open!");
            }
            command.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return;
    }

    /**
     * 缓存video num
     *
     * @param cacheKey
     * @param cacheValue
     */
    @Trace(dispatcher = true)
    public void setNumOfPubCache(String cacheKey, String cacheValue) {
        if (MXStringUtils.isBlank(cacheKey)) {
            return;
        }
        try {
            HystrixCommand<Void> command = new SetVideoNumOfPubCommand(cacheKey, cacheValue);
            if (command.isCircuitBreakerOpen()) {
                NewRelic.noticeError(command.getCommandKey().name() + " circuit breaker open!");
            }
            command.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取videoNum缓存结果
     *
     * @param cacheKey
     * @return
     */
    @Trace(dispatcher = true)
    public Integer getNumOfPubCache(String cacheKey) {
        if (MXStringUtils.isBlank(cacheKey)) {
            return null;
        }
        try {
            HystrixCommand<String> command = new GetVideoNumOfPubCommand(cacheKey);
            if (command.isCircuitBreakerOpen()) {
                NewRelic.noticeError(command.getCommandKey().name() + " circuit breaker open!");
            }
            String res = command.execute();
            if (MXStringUtils.isBlank(res) || !MXStringUtils.isNumeric(res)) {
                return null;
            }
            return Integer.parseInt(res);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 从缓存中取出Result list
     *
     * @param dc
     */
    @Trace(dispatcher = true)
    public Boolean getResultCache(BaseDataCollection dc) {
        try {
            HystrixCommand<Boolean> command = new GetUserResultListCommand(dc);
            if (command.isCircuitBreakerOpen()) {
                NewRelic.noticeError(command.getCommandKey().name() + " circuit breaker open!");
            }
            return command.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 用lettuce实现的result缓存, set方法
     */
    @Trace(dispatcher = true)
    public void setResultCacheByLettuce(BaseDataCollection dc) {
        if (MXJudgeUtils.isEmpty(dc.cachedResultList)) {
            return;
        }
        try {
            HystrixCommand<Void> command = new SetUserResultListLettuceCommand(dc);
            if (command.isCircuitBreakerOpen()) {
                NewRelic.noticeError(command.getCommandKey().name() + " circuit breaker open!");
            }
            command.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 用lettuce实现的result缓存, get方法
     */
    @Trace(dispatcher = true)
    public boolean getResultCacheByLettuce(BaseDataCollection dc) {
        try {
            HystrixCommand<Boolean> command = new GetUserResultListLettuceCommand(dc);
            if (command.isCircuitBreakerOpen()) {
                NewRelic.noticeError(command.getCommandKey().name() + " circuit breaker open!");
            }

            return command.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    @Trace(dispatcher = true)
    public Cache<String, List<Result>> getUserResultCache() {
        return userResultCache;
    }

    @Trace(dispatcher = true)
    public String getString(String key) {
        try {
            HystrixCommand<String> command = new StringCommand(key);

            if (command.isCircuitBreakerOpen()) {
                NewRelic.noticeError(command.getCommandKey().name() + " circuit breaker open!");
            }

            return command.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return "";
    }

    @Trace(dispatcher = true)
    public List<String> getSetFromStg(String key) {
        try {
            HystrixCommand<List<String>> command = new SmembersCommand(key);
            if (command.isCircuitBreakerOpen()) {
                NewRelic.noticeError(command.getCommandKey().name() + " circuit breaker open!");
            }
            return command.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return Collections.emptyList();
    }

    @Trace(dispatcher = true)
    public String getStringFromStg(String key) {
        try {
            HystrixCommand<String> command = new GetStringStgCommand(key);
            if (command.isCircuitBreakerOpen()) {
                NewRelic.noticeError(command.getCommandKey().name() + " circuit breaker open!");
            }
            return command.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return "";
    }

    public List<String> getVideoFeatureZsetInfoFromRedis(String redisKey) {
        try {
            HystrixCommand<List<String>> command = new VideoFeatureZsetInfoCommand(redisKey);
            if (command.isCircuitBreakerOpen()) {
                NewRelic.noticeError(command.getCommandKey().name() + " circuit breaker open!");
            }
            return command.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public Map<String, SageMakerPublisherFeatureDocument> getPubFeatureInfoFromRedis(Set<String> publisherIDs) {
        Map<String, SageMakerPublisherFeatureDocument> totalResult = new HashMap<>(200);
        if (MXJudgeUtils.isNotEmpty(publisherIDs)) {

            Map<String, SageMakerPublisherFeatureDocument> docFromCache = MXDataSource.cache().getSageMakerPubFeatureFromCache(publisherIDs);
            if (docFromCache != null) {
                publisherIDs.removeAll(docFromCache.keySet());
                totalResult.putAll(docFromCache);
            }

            Map<String, SageMakerPublisherFeatureDocument> resultMap = new ConcurrentHashMap<>();
            List<String> fields = Arrays.asList("view_pv", "loop_play_rate", "list_ctr", "finish_rate", "like_rate", "download_rate",
                    "share_rate", "total_follower", "video_count", "daily_new_videos", "play_rate", "avg_playtime");
            publisherIDs.parallelStream().forEach((publisherID) -> {
                try {
                    String redisKey = String.format("pubf:%s:7d", publisherID);
                    HystrixCommand<Map<String, String>> command = new HMgetForClusterCommand(HystrixUtil.pubFeatureRedisSetter, redisKey, fields, this::getPubFeatureClusterConnection);
                    Map<String, String> redisResutl = command.execute();
                    if (MapUtils.isNotEmpty(redisResutl)) {
                        SageMakerPublisherFeatureDocument pubFeatureDoc = new SageMakerPublisherFeatureDocument();
                        if (redisResutl.containsKey("view_pv") && MXStringUtils.isNotEmpty(redisResutl.get("view_pv"))) {
                            pubFeatureDoc.setPubView(Integer.parseInt(redisResutl.get("view_pv")));
                        }

                        if (redisResutl.containsKey("loop_play_rate") && MXStringUtils.isNotEmpty(redisResutl.get("loop_play_rate"))) {
                            pubFeatureDoc.setPubLoopPlayRate(Double.parseDouble(redisResutl.get("loop_play_rate")));
                        }

                        if (redisResutl.containsKey("list_ctr") && MXStringUtils.isNotEmpty(redisResutl.get("list_ctr"))) {
                            pubFeatureDoc.setPubListCtr(Double.parseDouble(redisResutl.get("list_ctr")));
                        }

                        if (redisResutl.containsKey("finish_rate") && MXStringUtils.isNotEmpty(redisResutl.get("finish_rate"))) {
                            pubFeatureDoc.setPubFinishRate(Double.parseDouble(redisResutl.get("finish_rate")));
                        }

                        if (redisResutl.containsKey("like_rate") && MXStringUtils.isNotEmpty(redisResutl.get("like_rate"))) {
                            pubFeatureDoc.setPubLikeRate(Double.parseDouble(redisResutl.get("like_rate")));
                        }

                        if (redisResutl.containsKey("download_rate") && MXStringUtils.isNotEmpty(redisResutl.get("download_rate"))) {
                            pubFeatureDoc.setPubDownloadRate(Double.parseDouble(redisResutl.get("download_rate")));
                        }

                        if (redisResutl.containsKey("share_rate") && MXStringUtils.isNotEmpty(redisResutl.get("share_rate"))) {
                            pubFeatureDoc.setPubShareRate(Double.parseDouble(redisResutl.get("share_rate")));
                        }

                        if (redisResutl.containsKey("total_follower") && MXStringUtils.isNotEmpty(redisResutl.get("total_follower"))) {
                            pubFeatureDoc.setPubFollowerAll(Integer.parseInt(redisResutl.get("total_follower")));
                        }

                        if (redisResutl.containsKey("video_count") && MXStringUtils.isNotEmpty(redisResutl.get("video_count"))) {
                            pubFeatureDoc.setPubTotalVideos(Integer.parseInt(redisResutl.get("video_count")));
                        }

                        if (redisResutl.containsKey("daily_new_videos") && MXStringUtils.isNotEmpty(redisResutl.get("daily_new_videos"))) {
                            pubFeatureDoc.setPubDailyNewVideos(Double.parseDouble(redisResutl.get("daily_new_videos")));
                        }

                        if (redisResutl.containsKey("play_rate") && MXStringUtils.isNotEmpty(redisResutl.get("play_rate"))) {
                            pubFeatureDoc.setPubPlayRate(Double.parseDouble(redisResutl.get("play_rate")));
                        }

                        if (redisResutl.containsKey("avg_playtime") && MXStringUtils.isNotEmpty(redisResutl.get("avg_playtime"))) {
                            pubFeatureDoc.setPubAVGPlayTime(Double.parseDouble(redisResutl.get("avg_playtime")));
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

    public Map<String, Map<String, Double>> getFieldConf(String fieldConf) {
        Map<String, Map<String, Double>> u = new HashMap<>();
        HmgetAllCommand command = new HmgetAllCommand(
                HystrixUtil.fieldConfSetter,
                fieldConf,
                this::getStrategyConnection);

        Map<String, String> r = command.execute();
        if (!OptionalUtil.ofNullable(r).isPresent() || !r.containsKey("base")) {
            return u;
        }

        String s = r.get("base");
        if (MXStringUtils.isEmpty(s)) {
            return u;
        }
        u.put("base", getConf(s));
        r.entrySet().stream().filter(m -> !"base".equals(m.getKey()))
                .forEach(m2 -> {
                    Map<String, Double> map = new HashMap<>(u.get("base"));
                    Map<String, Double> newMap = getConf(m2.getValue());
                    newMap.forEach(map::put);
                    u.put(m2.getKey(), map);
                });
        return u;
    }

    private Map<String, Double> getConf(String conf) {
        Map<String, Double> temp = new HashMap<>();
        JSONObject o = JSONObject.parseObject(conf);
        o.forEach((k, v) -> temp.put(k, Double.parseDouble(String.valueOf(v))));
        return temp;
    }
}
