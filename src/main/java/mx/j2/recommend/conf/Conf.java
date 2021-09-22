package mx.j2.recommend.conf;

import com.alibaba.fastjson.JSONObject;
import com.newrelic.api.agent.NewRelic;
import com.newrelic.api.agent.Trace;
import mx.j2.recommend.util.MXJudgeUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * 系统配置
 *
 * @author zhuowei
 */
public class Conf {
    private static Map<String, String> hostAndPortMap;

    /**
     * 工作线程等配置
     */
    private static int workThreadNum;
    private static int selectorThreadNum;
    private static int acceptQueueSizePerThread;

    /**
     * 端口
     */
    private static int port;

    /**
     * 日志文件
     */
    private static String log4jConf;

    /**
     * 推荐流文件
     */
    private static String recommendFlowConfFile;

    /**
     * 业务配置文件
     */
    private static String businessConfFile;

    /**
     * 用于业务缓存的redis信息
     */
    private static String redisCacheHost;
    private static int redisCachePort;

    private static String redisCacheNewHost;
    private static int redisCacheNewPort;

    private static String redisCacheTopHotHost;
    private static int redisCacheTopHotPort;

    /**
     * 用于布隆过滤redis Master
     */
    private static String redisBloomMasterHost;
    private static int redisBloomMasterPort;

    /**
     * 用于布隆过滤redis Slave
     */
    private static String redisBloomSlaveHost;
    private static int redisBloomSlavePort;

    /**
     * 用于Guava bloom集群
     */
    private static String redisGuavaHost;
    private static int redisGuavaPort;

    /**
     * 存储click count等特征的redis
     */
    private static String redisStrategyHost;
    private static int redisStrategyPort;

    /**
     * 存储了所有的私密账户
     */
    private static String redisPrivateAccountHost;
    private static int redisPrivateAccountPort;

    private static String pubFeatureRedisHost;
    private static int pubFeatureRedisPort;

    /**
     * 限流器最大令牌数
     */
    private static Double totalRateForAllInterfaces;

    /**
     * 内部接口限流器最大令牌数
     */
    private static Double totalRateForInternalInterfaces;

    /**
     * es配置相关配置:服务地址/
     */
    private static String elasticSearchEndPointURL;
    private static String elasticSearchEndPointPort;
    private static String videoElasticSearchEndPointURL;
    private static String videoElasticSearchEndPointPort;
    private static String strategyElasticSearchEndPointURL;
    private static String strategyElasticSearchEndPointPort;
    private static String videoElasticSearchVersion7EndPointURL;
    private static String videoElasticSearchVersion7Port;

    private static String inactiveUsersHistoryBloomRedisEndPointURL;
    private static int inactiveUsersHistoryBloomRedisPort;

    /**
     * 用户历史过期时间
     */
    private static int historyIdsExpireTime;

    /**
     * 推荐结果过期时间
     */
    private static int recommendResultCacheExpireTime;

    /**
     * 获取videoNum的过期时间
     */
    private static int recommendVideoNumExpireTime;
    /**
     * redis中历史推荐记录的key的后缀
     */
    private static String historyIdsSuffix;

    /**
     * 推荐结果缓存的开关（开为on或ON）
     */
    private static String recommendResultCacheSwitch;

    /**
     * 不活跃用户历史，从 ca 取的开关
     */
    private static String inactiveUsersHistoryFromCassandraSwitch;

    /**
     * 用户历史展现数据缓存队列的最大长度
     */
    private static Integer maxHistoryListSize;

    /**
     *
     */
    private static Integer historySize;

    /**
     * redis中推荐结果cache的key的后缀
     */
    private static String recommendResultCacheSuffix;

    /**
     * 默认请求数
     */
    private static int defaultRequestNumber;

    /**
     * ES索引
     */
    private static String mxIndex;

    /**
     * CMS 大ES索引
     */
    private static String cmsIndex;

    /**
     * 一级流量池 索引
     */
    private static String ugcLv1Index;

    /**
     * 音乐
     */
    private static String musicIndex;

    /**
     * 本地缓存开关
     */
    private static boolean localCacheSwitch;

    /**
     * 环境
     */
    private static String env;

    /**
     * 子环境
     * 当 env=prod 时，subEnv 可能为 subEnv=pre 预发布环境
     */
    private static String subEnv;

    /**
     * 用户漂流瓶历史后缀
     */
    private static String userBottlesHistorySuffix;

    /**
     * video排序配置
     */
    private static Map<String, Integer> videoConfurationMap;

    /**
     * get some one's followers Service url
     */
    private static String mxFollowerServerUrl;

    private static int serverTransportClientTimeout;
    private static int serverTransportBackLog;

    private static String poolConf;
    private static String strategyPoolConf;
    private static String exposurePoolConf;
    private static String profilePoolConf;

    /**
     * redis相关配置
     */
    private static int restClientMaxRetryTimeoutMillis;
    private static int restClientHttpConfigIoThreadCount;
    private static int restClientHttpConfigSelectInterval;
    private static boolean restClientHttpConfigTcpNoDelay;
    private static boolean restClientHttpConfigSoReuseAddress;
    private static int restClientHttpConfigConnectTimeout;
    private static int restClientHttpConfigSoTimeout;

    private static int restClientRequestConfigConnectionRequestTimeout;
    private static int restClientRequestConfigSocketTimeout;

    private static int genericObjectPoolConfigRemoveAbandonedTimeout;
    private static boolean genericObjectPoolConfigBlockWhenExhausted;

    private static int elasticsearchAsynTimeout;
    private static int jedisClusterSocketTimeout;

    // 在7分段的推荐结果中, 混入的tiktok内容的比例, 哪怕分数不够7分
    private static float tiktokContentRatio;

    // publisher_videos的拉键词典
    private static String publisherVideosConf;

    /**
     * 短视频保底数据，存的是视频 id
     */
    private static String fallbackVideosConf;

    private static long requestTimeOut;

    // AWS SNS服务
    private static String awsAccessKeyId;
    private static String awsSecretAccessKey;
    private static String topicArn;
    private static String topicArnPublisher;
    private static String videoNumSqsUrl;

    private static String cassandraHostUrl;
    private static String cassandraHostPort;
    private static String cassandraDc;
    private static String newBloomCassandraDc;

    private static String strategyCassandraHostUrl;
    private static String strategyCassandraHostPort;
    private static String strategyCassandraDc;

    private static String strategyTagCassandraHost;
    private static String strategyTagCassandraPort;
    private static String strategyTagCassandraDc;

    private static String strategyRealTimeCassandraHost;
    private static String strategyRealTimeCassandraPort;
    private static String strategyRealTimeCassandraDc;

    private static String historyCassandraHostUrl;
    private static String newHistoryCaHostUrl;
    private static String newHistoryCaHostPort;
    private static String historyCassandraHostPort;

    private static String publisherCassandraHostUrl;
    private static String publisherCassandraHostPort;

    private static String publisherPageCassandraHost;
    private static String publisherPageCassandraPort;
    private static String publisherPageCassandraDc;

    private static String needFilterVideoConfPath;

    /**
     * language name 转 id 配置文件
     */
    private static String languageNameToIdConfPath;

    /**
     * 用户选择的语言配置文件
     */
    private static String userSelectLanguageConfPath;

    /**
     * 热门城市和州配置文件
     */
    private static String topCityAndStateConfPath;

    /**
     * 获取两个用户是否为好友（互相关注）
     */
    private static String mxFriendListServerUrl;

    /**
     * 获取两个用户是否拉黑（单向拉黑便算作拉黑）
     */
    private static String mxIsBlockServerUrl;
    /**
     * 是否关注
     */
    private static String mxIsFollowedServerUrl;

    /**
     * Follow Card 中 过滤RMBlock数据
     */
    private static String mxRemoveBlockServerUrl;

    /**
     * debug专用的一些信息, 临时调试使用
     */
    private static JSONObject debugInfo;

    /**
     * 读取配置
     *
     * @return
     */
    @Trace(dispatcher = true)
    public static boolean loadConf(String cf) {
        FileInputStream inputStream = null;
        try {
            //private conf
            inputStream = new FileInputStream(new File(cf));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return false;
        }
        Properties p = new Properties();
        try {
            p.load(inputStream);
            hostAndPortMap = new HashMap<>();
            // ENV
            env = p.getProperty("env", "DEV");
            // sub env
            subEnv = p.getProperty("subEnv", "");

            // redisCache
            parstHostAndPort(p.getProperty("redisCacheHOST" + env), "6379");
            if (hostAndPortMap.isEmpty()) {
                System.out.print("redisCacheHost not correct");
                return false;
            } else {
                redisCacheHost = hostAndPortMap.get("host");
                redisCachePort = Integer.parseInt(hostAndPortMap.get("port"));
            }

            // redisCache
            parstHostAndPort(p.getProperty("redisCacheNewHOST" + env), "6379");
            if (hostAndPortMap.isEmpty()) {
                System.out.print("redisCacheHost not correct");
                return false;
            } else {
                redisCacheNewHost = hostAndPortMap.get("host");
                redisCacheNewPort = Integer.parseInt(hostAndPortMap.get("port"));
            }

            //redisTopHot
            parstHostAndPort(p.getProperty("redisCacheTopHotHOST" + env), "6379");
            if (hostAndPortMap.isEmpty()) {
                System.out.print("redisCacheHost not correct");
                return false;
            } else {
                redisCacheTopHotHost = hostAndPortMap.get("host");
                redisCacheTopHotPort = Integer.parseInt(hostAndPortMap.get("port"));
            }

            mxFollowerServerUrl = p.getProperty("mxFollowerServerUrl" + env);

            // is friend
            mxFriendListServerUrl = p.getProperty("mxFriendListServerUrl" + env);

            // is block
            mxIsBlockServerUrl = p.getProperty("mxIsBlockServerUrl" + env);

            mxIsFollowedServerUrl = p.getProperty("mxIsFollowedServerUrl" + env);

            mxRemoveBlockServerUrl = p.getProperty("mxRemoveBlockServerUrl" + env);

            // redis Bloom
            parstHostAndPort(p.getProperty("redisBloomMasterHOST" + env), "6379");
            if (hostAndPortMap.isEmpty()) {
                System.out.print("redisBloomMasterHost not correct");
                return false;
            } else {
                redisBloomMasterHost = hostAndPortMap.get("host");
                redisBloomMasterPort = Integer.parseInt(hostAndPortMap.get("port"));
            }
            parstHostAndPort(p.getProperty("redisBloomSlaveHOST" + env), "6379");
            if (hostAndPortMap.isEmpty()) {
                System.out.print("redisBloomSlaveHost not correct");
                return false;
            } else {
                redisBloomSlaveHost = hostAndPortMap.get("host");
                redisBloomSlavePort = Integer.parseInt(hostAndPortMap.get("port"));
            }

            //redis guava bloom
            parstHostAndPort(p.getProperty("redisGuavaHOST" + env), "6379");
            if (hostAndPortMap.isEmpty()) {
                System.out.print("redisGuavaHOST not correct");
                return false;
            } else {
                redisGuavaHost = hostAndPortMap.get("host");
                redisGuavaPort = Integer.parseInt(hostAndPortMap.get("port"));
            }

            //redis UninterestVideoHost
            parstHostAndPort(p.getProperty("inactiveUsersHistoryBloomRedis" + env), "6379");
            if (hostAndPortMap.isEmpty()) {
                System.out.print("inactiveUsersHistoryBloomRedis not correct");
                return false;
            } else {
                inactiveUsersHistoryBloomRedisEndPointURL = hostAndPortMap.get("host");
                inactiveUsersHistoryBloomRedisPort = Integer.parseInt(hostAndPortMap.get("port"));
            }


            // redisStrategy
            parstHostAndPort(p.getProperty("redisStrategyHost" + env), "6379");
            if (hostAndPortMap.isEmpty()) {
                System.out.print("redisStrategyHost not correct");
                return false;
            } else {
                redisStrategyHost = hostAndPortMap.get("host");
                redisStrategyPort = Integer.parseInt(hostAndPortMap.get("port"));
            }

            // redis for private accounts
            parstHostAndPort(p.getProperty("redisPrivateAccountHOST" + env), "6379");
            if (hostAndPortMap.isEmpty()) {
                System.out.println("redisPrivateAccountHost not correct");
                return false;
            } else {
                redisPrivateAccountHost = hostAndPortMap.get("host");
                redisPrivateAccountPort = Integer.parseInt(hostAndPortMap.get("port"));
            }

            parstHostAndPort(p.getProperty("redisPubFeatureHost" + env), "6379");
            if (hostAndPortMap.isEmpty()) {
                System.out.println("pubFeatureRedisHost not correct");
                return false;
            } else {
                pubFeatureRedisHost = hostAndPortMap.get("host");
                pubFeatureRedisPort = Integer.parseInt(hostAndPortMap.get("port"));
            }

            //  elasticSearch
            parstHostAndPort(p.getProperty("elasticSearchHOST" + env), "9200");
            if (hostAndPortMap.isEmpty()) {
                System.out.print("elasticSearchHOST not correct");
                return false;
            } else {
                elasticSearchEndPointURL = hostAndPortMap.get("host");
                elasticSearchEndPointPort = hostAndPortMap.get("port");
            }
            // AWS key
            awsAccessKeyId = p.getProperty("awsAccessKeyId" + env);
            awsSecretAccessKey = p.getProperty("awsSecretAccessKey" + env);
            topicArn = p.getProperty("topicArn" + env);
            topicArnPublisher = p.getProperty("topicArnPublisher" + env);
            videoNumSqsUrl = p.getProperty("videoNumSqsUrl" + env);
            //  videoElasticSearch  strategyElasticSearchHOST
            parstHostAndPort(p.getProperty("videoElasticSearchHOST" + env), "9200");
            if (hostAndPortMap.isEmpty()) {
                System.out.print("videoElasticSearchHOST not correct");
                return false;
            } else {
                videoElasticSearchEndPointURL = hostAndPortMap.get("host");
                videoElasticSearchEndPointPort = hostAndPortMap.get("port");
            }

            parstHostAndPort(p.getProperty("videoElasticSearchVersion7HOST" + env), "9200");
            if (hostAndPortMap.isEmpty()) {
                System.out.print("videoElasticSearchVersion7HOST not correct");
                return false;
            } else {
                videoElasticSearchVersion7EndPointURL = hostAndPortMap.get("host");
                videoElasticSearchVersion7Port = hostAndPortMap.get("port");
            }

            //  strategyElasticSearchHOST
            parstHostAndPort(p.getProperty("strategyElasticSearchHOST" + env), "9200");
            if (hostAndPortMap.isEmpty()) {
                System.out.print("strategyElasticSearchHOST not correct");
                return false;
            } else {
                strategyElasticSearchEndPointURL = hostAndPortMap.get("host");
                strategyElasticSearchEndPointPort = hostAndPortMap.get("port");
            }

            //  cassandra
            parstHostAndPort(p.getProperty("cassandraHost" + env), "9042");
            if (hostAndPortMap.isEmpty()) {
                System.out.print("cassandraHost not correct");
                return false;
            } else {
                cassandraHostUrl = hostAndPortMap.get("host");
                cassandraHostPort = hostAndPortMap.get("port");
            }
            cassandraDc = p.getProperty("cassandraDc" + env, "dc1");

            //  cassandra
            parstHostAndPort(p.getProperty("historyBloomCassandra" + env), "9042");
            if (hostAndPortMap.isEmpty()) {
                System.out.print("historyBloomCassandra not correct");
                return false;
            } else {
                historyCassandraHostUrl = hostAndPortMap.get("host");
                historyCassandraHostPort = hostAndPortMap.get("port");
            }

            //new bloom
            parstHostAndPort(p.getProperty("newHistoryBloomCa" + env), "9042");
            if (hostAndPortMap.isEmpty()) {
                System.out.print("historyBloomCassandra not correct");
                return false;
            } else {
                newHistoryCaHostUrl = hostAndPortMap.get("host");
                newHistoryCaHostPort = hostAndPortMap.get("port");
            }
            newBloomCassandraDc = p.getProperty("newBloomCassandraDc" + env, "ap-south");
            //  strategyCassandra
            parstHostAndPort(p.getProperty("strategyCassandraHost" + env), "9042");
            if (hostAndPortMap.isEmpty()) {
                System.out.print("strategyCassandraHost not correct");
                return false;
            } else {
                strategyCassandraHostUrl = hostAndPortMap.get("host");
                strategyCassandraHostPort = hostAndPortMap.get("port");
            }

            strategyCassandraDc = p.getProperty("strategyCassandraDc" + env, "dc1");

            parstHostAndPort(p.getProperty("strategyTagCassandraHost" + env), "9042");
            if (hostAndPortMap.isEmpty()) {
                System.out.print("strategyTagCassandraHost not correct");
                return false;
            } else {
                strategyTagCassandraHost = hostAndPortMap.get("host");
                strategyTagCassandraPort = hostAndPortMap.get("port");
            }

            publisherPageCassandraDc = p.getProperty("publisherPageCassandraDc" + env, "ap-south");

            parstHostAndPort(p.getProperty("publisherPageCassandraHost" + env), "9042");
            if (hostAndPortMap.isEmpty()) {
                System.out.print("strategyTagCassandraHost not correct");
                return false;
            } else {
                publisherPageCassandraHost = hostAndPortMap.get("host");
                publisherPageCassandraPort = hostAndPortMap.get("port");
            }

            strategyTagCassandraDc = p.getProperty("strategyTagCassandraDc" + env, "dc1");

            parstHostAndPort(p.getProperty("strategyRealTimeCassandraHost" + env), "9042");
            if (hostAndPortMap.isEmpty()) {
                System.out.print("strategyRealTimeCassandraHost not correct");
                return false;
            } else {
                strategyRealTimeCassandraHost = hostAndPortMap.get("host");
                strategyRealTimeCassandraPort = hostAndPortMap.get("port");
            }

            strategyRealTimeCassandraDc = p.getProperty("strategyRealTimeCassandraDc" + env, "dc1");

            parstHostAndPort(p.getProperty("publisherCassandra" + env), "9042");
            if (hostAndPortMap.isEmpty()) {
                System.out.print("publisherCassandra not correct");
                return false;
            } else {
                publisherCassandraHostUrl = hostAndPortMap.get("host");
                publisherCassandraHostPort = hostAndPortMap.get("port");
            }
            // ES index
            mxIndex = p.getProperty("mxIndex" + env);
            cmsIndex = p.getProperty("cmsIndex" + env);
            // ugc lv1 Index
            ugcLv1Index = p.getProperty("ugcLv1Index" + env);
            musicIndex = p.getProperty("musicIndex" + env);
            // Port
            int defaultPort = 19889;
            try {
                String portStr;
                portStr = System.getenv(p.getProperty("port" + env, "RECOMMENDATION_PORT"));

                if (MXJudgeUtils.isNotEmpty(portStr)) {
                    port = Integer.parseInt(portStr);
                } else {
                    port = defaultPort;
                }
            } catch (Exception e) {
                System.out.print(p.getProperty("port" + env) + " is not a parsable port, use default port: 91889");
                port = defaultPort;
            }

            if (p.containsKey("debugInfo")) {
                try {
                    String debugRaw = p.getProperty("debugInfo", "{}");
                    debugInfo = JSONObject.parseObject(debugRaw);
                } catch (Exception e) {
                    debugInfo = new JSONObject();
                }
            }

            // get business conf
            businessConfFile = p.getProperty("businessConfFile");
            boolean b = loadBusinessConf(businessConfFile);
            if (b) {
                return true;
            } else {
                return false;
            }
        } catch (IOException e1) {
            e1.printStackTrace();
        } finally {
            try {
                inputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return false;
    }


    public static boolean loadBusinessConf(String conf) {
        FileInputStream busInputStream = null;
        try {
            //business conf
            busInputStream = new FileInputStream(new File(conf));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return false;
        }
        Properties p = new Properties();
        try {
            p.load(busInputStream);

            // file path
            log4jConf = p.getProperty("log4jConf");
            recommendFlowConfFile = p.getProperty("recommendFlowConf");
            publisherVideosConf = p.getProperty("publisherVideosConf");
            poolConf = p.getProperty("poolConf");
            strategyPoolConf = p.getProperty("strategyPoolConf");
            exposurePoolConf = p.getProperty("exposurePoolConf");
            profilePoolConf = p.getProperty("profilePoolConf");
            fallbackVideosConf = p.getProperty("fallbackVideosConf");
            needFilterVideoConfPath = p.getProperty("needFilterVideoConfPath");
            languageNameToIdConfPath = p.getProperty("languageNameToIdConfPath");
            userSelectLanguageConfPath = p.getProperty("userSelectLanguageConfPath");
            topCityAndStateConfPath = p.getProperty("topCityAndStateConfPath");

            // thread
            workThreadNum = Integer.parseInt(p.getProperty("workThreadNum"));
            selectorThreadNum = Integer.parseInt(p.getProperty("selectorThreadNum"));
            acceptQueueSizePerThread = Integer.parseInt(p.getProperty("acceptQueueSizePerThread"));

            // redis expire time
            historyIdsExpireTime = Integer.parseInt(p.getProperty("historyIdsExpireTime", "604800"));
            recommendResultCacheExpireTime = Integer.parseInt(p.getProperty("recommendResultCacheExpireTime", "10"));
            recommendVideoNumExpireTime = Integer.parseInt(p.getProperty("recommendVideoNumExpireTime", "5"));
            // redis suffix
            historyIdsSuffix = p.getProperty("historyIdsSuffix", "his");
            recommendResultCacheSuffix = p.getProperty("recommendResultCacheSuffix", "rrcs");

            recommendResultCacheSwitch = p.getProperty("recommendResultCacheSwitch");

            defaultRequestNumber = Integer.parseInt(p.getProperty("defaultRequestNumber"));

            historySize = Integer.parseInt(p.getProperty("historySize"));
            if (p.containsKey("maxHistoryListSize")) {
                maxHistoryListSize = Integer.parseInt(p.getProperty("maxHistoryListSize", "20000"));
            } else {
                maxHistoryListSize = 20000;
            }

            userBottlesHistorySuffix = p.getProperty("userBottlesHistorySuffix");

            localCacheSwitch = Boolean.parseBoolean(p.getProperty("localCacheSwitch"));

            serverTransportClientTimeout = Integer.parseInt(p.getProperty("serverTransportClientTimeout"));
            serverTransportBackLog = Integer.parseInt(p.getProperty("serverTransportBackLog"));

            restClientMaxRetryTimeoutMillis = Integer.parseInt(p.getProperty("restClientMaxRetryTimeoutMillis"));
            restClientHttpConfigIoThreadCount = Integer.parseInt(p.getProperty("restClientHttpConfigIoThreadCount"));
            restClientHttpConfigSelectInterval = Integer.parseInt(p.getProperty("restClientHttpConfigSelectInterval"));
            restClientHttpConfigTcpNoDelay = Boolean.parseBoolean(p.getProperty("restClientHttpConfigTcpNoDelay"));
            restClientHttpConfigSoReuseAddress = Boolean.parseBoolean(p.getProperty("restClientHttpConfigSoReuseAddress"));
            restClientHttpConfigConnectTimeout = Integer.parseInt(p.getProperty("restClientHttpConfigConnectTimeout"));
            restClientHttpConfigSoTimeout = Integer.parseInt(p.getProperty("restClientHttpConfigSoTimeout"));
            restClientRequestConfigConnectionRequestTimeout = Integer.parseInt(p.getProperty("restClientRequestConfigConnectionRequestTimeout"));
            restClientRequestConfigSocketTimeout = Integer.parseInt(p.getProperty("restClientRequestConfigSocketTimeout"));

            genericObjectPoolConfigRemoveAbandonedTimeout = Integer.parseInt(p.getProperty("genericObjectPoolConfigRemoveAbandonedTimeout"));
            genericObjectPoolConfigBlockWhenExhausted = Boolean.parseBoolean(p.getProperty("genericObjectPoolConfigBlockWhenExhausted"));

            elasticsearchAsynTimeout = Integer.parseInt(p.getProperty("elasticsearchAsynTimeout"));
            jedisClusterSocketTimeout = Integer.parseInt(p.getProperty("jedisClusterSocketTimeout"));

            totalRateForAllInterfaces = Double.parseDouble(p.getProperty("totalRateForAllInterfaces"));

            totalRateForInternalInterfaces = Double.parseDouble(p.getProperty("totalRateForInternalInterfaces"));

            tiktokContentRatio = Float.parseFloat(p.getProperty("tiktokContentRatio"));

            requestTimeOut = Long.parseLong(p.getProperty("requestTimeOut"));

            inactiveUsersHistoryFromCassandraSwitch = p.getProperty("inactiveUsersHistoryFromCassandraSwitch");

            String videoCongurationString = p.getProperty("videosConfiguration");
            if (MXJudgeUtils.isNotEmpty(videoCongurationString)) {
                videoConfurationMap = new HashMap<>();
                String[] array = videoCongurationString.split(",");
                for (int i = 0; i < array.length; i++) {
                    if (array[i].contains(":")) {
                        String[] everyConguration = array[i].split(":");
                        if (everyConguration.length < 2) {
                            continue;
                        }
                        videoConfurationMap.put(everyConguration[0], Integer.parseInt(everyConguration[1]));
                    }
                }
            }

            return true;
        } catch (IOException e1) {
            e1.printStackTrace();
            NewRelic.noticeError("get conf error, exception -> " + e1);
        } finally {
            try {
                busInputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return false;
    }

    private static void parstHostAndPort(String hostAndPort, String defaultPort) {
        hostAndPortMap.clear();
        String[] hostArray = hostAndPort.split(":");
        if (hostArray.length == 2) {
            hostAndPortMap.put("host", hostArray[0]);
            hostAndPortMap.put("port", hostArray[1]);
        } else if (hostArray.length == 1) {
            hostAndPortMap.put("host", hostArray[0]);
            hostAndPortMap.put("port", defaultPort);
        } else {
            System.out.print("parse host and port failed -> " + hostAndPort);
        }
    }

    public static String getEnv() {
        return env;
    }

    public static String getSubEnv() {
        return subEnv;
    }

    public static int getWorkThreadNum() {
        return workThreadNum;
    }

    public static int getSelectorThreadNum() {
        return selectorThreadNum;
    }

    public static int getPort() {
        return port;
    }

    public static String getRecommendFlowConfFile() {
        return recommendFlowConfFile;
    }

    public static String getPublisherVideosConf() {
        return publisherVideosConf;
    }

    public static String getFallbackVideosConf() {
        return fallbackVideosConf;
    }

    public static String getElasticSearchEndPointURL() {
        return elasticSearchEndPointURL;
    }

    public static String getElasticSearchEndPointPort() {
        return elasticSearchEndPointPort;
    }

    public static String getVideoElasticSearchEndPointURL() {
        return videoElasticSearchEndPointURL;
    }

    public static String getVideoElasticSearchEndPointPort() {
        return videoElasticSearchEndPointPort;
    }

    public static int getRedisCachePort() {
        return redisCachePort;
    }

    public static String getRedisCacheHost() {
        return redisCacheHost;
    }

    public static String getRedisBloomMasterHost() {
        return redisBloomMasterHost;
    }

    public static int getRedisBloomMasterPort() {
        return redisBloomMasterPort;
    }

    public static String getRedisBloomSlaveHost() {
        return redisBloomSlaveHost;
    }

    public static int getRedisBloomSlavePort() {
        return redisBloomSlavePort;
    }


    public static int getHistoryIdsExpireTime() {
        return historyIdsExpireTime;
    }

    public static int getRecommendResultCacheExpireTime() {
        return recommendResultCacheExpireTime;
    }

    public static String getHistoryIdsSuffix() {
        return historyIdsSuffix;
    }

    public static String getRecommendResultCacheSwitch() {
        return recommendResultCacheSwitch;
    }

    public static Integer getMaxHistoryListSize() {
        return maxHistoryListSize;
    }

    public static int getDefaultRequestNumber() {
        return defaultRequestNumber;
    }

    public static String getRecommendResultCacheSuffix() {
        return recommendResultCacheSuffix;
    }

    public static String getMxIndex() {
        return mxIndex;
    }

    public static String getCmsIndex() {
        return cmsIndex;
    }

    public static String getUgcLv1Index() {
        return ugcLv1Index;
    }

    public static String getMusicIndex() {
        return musicIndex;
    }

    public static boolean getLocalCacheSwitch() {
        return localCacheSwitch;
    }

    public static int getAcceptQueueSizePerThread() {
        return acceptQueueSizePerThread;
    }

    public static int getServerTransportClientTimeout() {
        return serverTransportClientTimeout;
    }

    public static int getServerTransportBackLog() {
        return serverTransportBackLog;
    }

    public static int getRestClientHttpConfigSelectInterval() {
        return restClientHttpConfigSelectInterval;
    }

    public static int getRestClientMaxRetryTimeoutMillis() {
        return restClientMaxRetryTimeoutMillis;
    }

    public static int getRestClientHttpConfigIoThreadCount() {
        return restClientHttpConfigIoThreadCount;
    }

    public static boolean isRestClientHttpConfigTcpNoDelay() {
        return restClientHttpConfigTcpNoDelay;
    }

    public static int getGenericObjectPoolConfigRemoveAbandonedTimeout() {
        return genericObjectPoolConfigRemoveAbandonedTimeout;
    }

    public static boolean isRestClientHttpConfigSoReuseAddress() {
        return restClientHttpConfigSoReuseAddress;
    }

    public static int getRestClientHttpConfigConnectTimeout() {
        return restClientHttpConfigConnectTimeout;
    }

    public static int getRestClientHttpConfigSoTimeout() {
        return restClientHttpConfigSoTimeout;
    }

    public static int getRestClientRequestConfigConnectionRequestTimeout() {
        return restClientRequestConfigConnectionRequestTimeout;
    }

    public static int getRestClientRequestConfigSocketTimeout() {
        return restClientRequestConfigSocketTimeout;
    }

    public static int getElasticsearchAsynTimeout() {
        return elasticsearchAsynTimeout;
    }

    public static int getJedisClusterSocketTimeout() {
        return jedisClusterSocketTimeout;
    }

    public static boolean isGenericObjectPoolConfigBlockWhenExhausted() {
        return genericObjectPoolConfigBlockWhenExhausted;
    }

    public static Map<String, Integer> getVideoConfurationMap() {
        return videoConfurationMap;
    }

    public static String getUserBottlesHistorySuffix() {
        return userBottlesHistorySuffix;
    }

    public static String getRedisStrategyHost() {
        return redisStrategyHost;
    }

    public static int getRedisStrategyPort() {
        return redisStrategyPort;
    }

    public static String getRedisPrivateAccountHost() {
        return redisPrivateAccountHost;
    }

    public static String getPubFeatureRedisHost() {
        return pubFeatureRedisHost;
    }

    public static int getPubFeatureRedisPort() {
        return pubFeatureRedisPort;
    }

    public static float getTiktokContentRatio() {
        return tiktokContentRatio;
    }

    public static String getMxFollowerServerUrl() {
        return mxFollowerServerUrl;
    }

    public static long getRequestTimeOut() {
        return requestTimeOut;
    }

    public static double getTotalRateForAllInterfaces() {
        return totalRateForAllInterfaces.doubleValue();
    }

    public static double getTotalRateForInternalInterfaces() {
        return totalRateForInternalInterfaces.doubleValue();
    }

    public static String getRedisGuavaHost() {
        return redisGuavaHost;
    }

    public static int getRedisGuavaPort() {
        return redisGuavaPort;
    }


    public static String getRedisCacheNewHost() {
        return redisCacheNewHost;
    }

    public static String getRedisCacheTopHotHost() {
        return redisCacheTopHotHost;
    }

    public static String getCassandraHostUrl() {
        return cassandraHostUrl;
    }

    public static Integer getCassandraHostPort() {
        Integer port = 9042;
        try {
            port = Integer.parseInt(cassandraHostPort);
        } catch (Exception e) {
            System.out.printf("cassandra port incorrect, use default value 9042");
        }
        return port;
    }

    public static String getCassandraDc() {
        return cassandraDc;
    }

    public static String getPoolConf() {
        return poolConf;
    }

    public static String getStrategyPoolConf() {
        return strategyPoolConf;
    }

    public static String getExposurePoolConf() {
        return exposurePoolConf;
    }

    public static String getProfilePoolConf() {
        return profilePoolConf;
    }

    public static String getRedisCacheTempNewHost() {
        return getRedisCacheHost();
    }

    public static String getAwsAccessKeyId() {
        return awsAccessKeyId;
    }

    public static String getAwsSecretAccessKey() {
        return awsSecretAccessKey;
    }

    public static String getTopicArn() {
        return topicArn;
    }

    public static String getStrategyElasticSearchEndPointURL() {
        return strategyElasticSearchEndPointURL;
    }

    public static String getStrategyElasticSearchEndPointPort() {
        return strategyElasticSearchEndPointPort;
    }

    public static String getStrategyCassandraHostUrl() {
        return strategyCassandraHostUrl;
    }

    public static Integer getStrategyCassandraHostPort() {
        Integer port = 9042;
        try {
            port = Integer.parseInt(strategyCassandraHostPort);
        } catch (Exception e) {
            System.out.printf("strategyCassandra port incorrect, use default value 9042");
        }
        return port;
    }

    public static String getStrategyCassandraDc() {
        return strategyCassandraDc;
    }

    public static String getTopicArnPublisher() {
        return topicArnPublisher;
    }

    public static String getInactiveUsersHistoryFromCassandraSwitch() {
        return inactiveUsersHistoryFromCassandraSwitch;
    }

    public static String getInactiveUsersHistoryBloomRedisEndPointURL() {
        return inactiveUsersHistoryBloomRedisEndPointURL;
    }

    public static int getInactiveUsersHistoryBloomRedisPort() {
        return inactiveUsersHistoryBloomRedisPort;
    }

    public static String getMxFriendListServerUrl() {
        return mxFriendListServerUrl;
    }

    public static String getNeedFilterVideoConfPath() {
        return needFilterVideoConfPath;
    }

    public static String getLanguageNameToIdConfPath() {
        return languageNameToIdConfPath;
    }

    public static String getUserSelectLanguageConfPath() {
        return userSelectLanguageConfPath;
    }

    public static String getMxIsBlockServerUrl() {
        return mxIsBlockServerUrl;
    }

    public static String getVideoElasticSearchVersion7EndPointURL() {
        return videoElasticSearchVersion7EndPointURL;
    }

    public static String getVideoElasticSearchVersion7Port() {
        return videoElasticSearchVersion7Port;
    }

    public static String getHistoryCassandraHostUrl() {
        return historyCassandraHostUrl;
    }

    public static int getHistoryCassandraHostPort() {
        int port = 9042;
        try {
            port = Integer.parseInt(historyCassandraHostPort);
        } catch (Exception e) {
            System.out.print("strategyCassandra port incorrect, use default value 9042");
        }
        return port;
    }

    public static int getRecommendVideoNumExpireTime() {
        return recommendVideoNumExpireTime;
    }

    public static String getDebugInfo(String key) {
        if (null == debugInfo) {
            return null;
        }
        if (!debugInfo.containsKey(key)) {
            return null;
        }
        return debugInfo.getString(key);
    }

    public static String getMxIsFollowedServerUrl() {
        return mxIsFollowedServerUrl;
    }

    public static String getMxRemoveBlockServerUrl() {
        return mxRemoveBlockServerUrl;
    }

    public static void setMxRemoveBlockServerUrl(String mxRemoveBlockServerUrl) {
        Conf.mxRemoveBlockServerUrl = mxRemoveBlockServerUrl;
    }

    public static String getVideoNumSqsUrl() {
        return videoNumSqsUrl;
    }

    public static String getNewHistoryCaHostUrl() {
        return newHistoryCaHostUrl;
    }

    public static int getNewHistoryCaHostPort() {
        int port = 9042;
        try {
            port = Integer.parseInt(newHistoryCaHostPort);
        } catch (Exception e) {
            System.out.print("strategyCassandra port incorrect, use default value 9042");
        }
        return port;
    }

    public static String getNewBloomCassandraDc() {
        return newBloomCassandraDc;
    }

    public static String getPublisherCassandraHostUrl() {
        return publisherCassandraHostUrl;
    }

    public static int getPublisherCassandraHostPort() {
        int port = 9042;
        try {
            port = Integer.parseInt(publisherCassandraHostPort);
        } catch (Exception e) {
            System.out.print("strategyCassandra port incorrect, use default value 9042");
        }
        return port;
    }

    public static String getStrategyTagCassandraHost() {
        return strategyTagCassandraHost;
    }

    public static int getStrategyTagCassandraPort() {
        int port = 9042;
        try {
            port = Integer.parseInt(strategyTagCassandraPort);
        } catch (Exception e) {
            System.out.print("strategyTagCassandraPort port incorrect, use default value 9042");
        }
        return port;
    }

    public static String getStrategyTagCassandraDc() {
        return strategyTagCassandraDc;
    }


    public static String getStrategyRealTimeCassandraHost() {
        return strategyRealTimeCassandraHost;
    }

    public static int getStrategyRealTimeCassandraPort() {
        int port = 9042;
        try {
            port = Integer.parseInt(strategyRealTimeCassandraPort);
        } catch (Exception e) {
            System.out.print("strategyRealTimeCassandraPort port incorrect, use default value 9042");
        }
        return port;
    }

    public static String getStrategyRealTimeCassandraDc() {
        return strategyRealTimeCassandraDc;
    }

    public static String getTopCityAndStateConfPath() {
        return topCityAndStateConfPath;
    }

    public static String getPublisherPageCassandraHost() {
        return publisherPageCassandraHost;
    }

    public static int getPublisherPageCassandraPort() {
        int port = 9042;
        try {
            port = Integer.parseInt(publisherPageCassandraPort);
        } catch (Exception e) {
            System.out.print("publisherPageCassandraPort port incorrect, use default value 9042");
        }
        return port;
    }

    public static String getPublisherPageCassandraDc() {
        return publisherPageCassandraDc;
    }
}
