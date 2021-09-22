package mx.j2.recommend.data_source;

import com.alibaba.fastjson.JSONObject;
import com.newrelic.api.agent.NewRelic;
import com.newrelic.api.agent.Trace;
import mx.j2.recommend.conf.Conf;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.flow.RecommendFlow;
import mx.j2.recommend.data_model.flow.RecommendFlowParser;
import mx.j2.recommend.data_model.flow.RecommendFlowParserIf;
import mx.j2.recommend.data_model.flow.RecommendFlowParserV2;
import mx.j2.recommend.manager.IStreamComponentManager;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.manager.MXManager;
import mx.j2.recommend.manager.impl.InternalRecallManager;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.LogTool;
import mx.j2.recommend.util.MXJudgeUtils;
import mx.j2.recommend.util.MXStringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.concurrent.ThreadSafe;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@ThreadSafe
public class RecommendFlowDataSource extends BaseDataSource {
    private static Logger logger = LogManager.getLogger(RecommendFlowDataSource.class);

    // 定时解析任务刷新间隔
    private static final int REFRESH_INTERVAL_IN_SECONDS = 60;

    /**
     * name到推荐流映射
     */
    private Map<String, RecommendFlow> recommendFlowMap;

    /**
     * 用户命中区间到推荐流的映射
     * <p>
     * <interfaceName, <rangeSlotIndex, RecommendFlow>>
     * <p>
     * 其中 rangeSlotIndex 范围 [0-9999]
     */
    private Map<String, Map<Integer, RecommendFlow>> rangeToFlowMap;

    public final static int SMALL_FLOW_NAME_BASE_NUMBER = 10000;

    /**
     * 流组件管理器集合
     */
    private List<IStreamComponentManager> managers;

    // 使用本地配置标志
    private boolean useLocalConfig;

    /**
     * 构造函数
     *
     * @param
     */
    public RecommendFlowDataSource() {
        recommendFlowMap = new HashMap<>();
        rangeToFlowMap = new HashMap<>();
    }

    /**
     * 初始化可配置组件管理器列表
     */
    private void initStreamComponentManagers() {
        managers = new ArrayList<>();
        //managers.add(MXManager.preRecall());
        managers.add(MXManager.prepare());
        managers.add(MXManager.recall());
        managers.add(MXManager.mixer());
        managers.add(MXManager.filter());
        managers.add(InternalRecallManager.INSTANCE);
        managers.add(MXManager.fallback());
        managers.add(MXManager.packer());
        managers.add(MXManager.predictor());
        managers.add(MXManager.ranker());
        managers.add(MXManager.ruler());
        managers.add(MXManager.scorer());
    }

    /**
     * 初始化
     *
     * @param
     */
    public void init() {
        if (isUseLocalConfig()) {
            logger.info("Init conf local.");
            initConfLocal();
        } else {
            logger.info("Init conf remote.");
            initConfRemote();
        }
        logger.info("{\"dataSourceInfo\":\"[RecommendFlowDataSource init successfully]\"}");
    }

    /**
     * 本地解析入口
     */
    private void initConfLocal() {
        useLocalConfig = true;
        parseRecommendFlowV2();
    }

    /**
     * 远端解析入口
     */
    private void initConfRemote() {
        initStreamComponentManagers();
        initConf();
        startRefreshScheduledTask();
    }

    /**
     * 初始化配置
     */
    private void initConf() {
        /*
         * 尝试 3 次
         */
        int retry = -3;

        while (retry < 0) {
            if (parseConfRemote(true)) {
                break;// 成功，退出
            } else {
                retry++;
            }
        }

        // 3 次都失败了，杀死进程
        if (retry == 0) {
            LogTool.reportError(DefineTool.ErrorEnum.FATAL, logger, new Exception("Failed to parse remote flow config."));
        }
    }

    /**
     * 开启定时刷新配置任务
     */
    private void startRefreshScheduledTask() {
        ScheduledExecutorService serviceNormal = Executors.newSingleThreadScheduledExecutor();

        // 尝试 3 次
        int setScheduleSuccesfull = -3;
        while (setScheduleSuccesfull < 0) {
            try {
                serviceNormal.scheduleAtFixedRate(() -> parseConfRemote(false),
                        REFRESH_INTERVAL_IN_SECONDS,
                        REFRESH_INTERVAL_IN_SECONDS,
                        TimeUnit.SECONDS);
                setScheduleSuccesfull = 0;
            } catch (Exception e) {
                setScheduleSuccesfull++;
                e.printStackTrace();
                logger.error("Failed to start refresh flow config schedule task: " + e.getMessage());
            }
        }
    }

    /**
     * 解析远端配置
     */
    private boolean parseConfRemote(boolean init) {
        // 接口列表
        List<String> interfaceList;
        try {
            interfaceList = getInterfaceList(init);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Failed to get interface list: " + e.getMessage());
            return false;
        }

        if (MXJudgeUtils.isNotEmpty(interfaceList)) {
            // 遍历所有需要拉取配置的接口
            JSONObject flowConfIt = null;
            for (String interfaceIt : interfaceList) {
                // 拿远端的接口配置信息
                try {
                    flowConfIt = getFlowConfig(interfaceIt);

                    if (MXJudgeUtils.isEmpty(flowConfIt)) {
                        logger.error("Failed to get flow config for " + interfaceIt + " because empty.");
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    logger.error("Failed to get flow config for " + interfaceIt + " because " + e.getMessage());
                }

                // 解析配置信息
                if (!parseFlowConfig(interfaceIt, flowConfIt)) {// 如果解析失败了
                    logger.error("Failed to parse remote flow config for " + interfaceIt);

                    // 如果是服务刚启动，有一个错的就中断
                    if (init) {
                        return false;
                    }
                }
            }
        } else {// 运行中，且当前没有配置变化的接口
            logger.info("No changed interface.");
        }

        // 打印当前的配置集合
        logger.info("Current flow config: " + recommendFlowMap);

        return true;
    }

    /**
     * 拿接口列表
     *
     * @param init 是否是服务刚启动
     */
    private List<String> getInterfaceList(boolean init) {
        if (init) {// 服务刚启动，从本地配置中拿所有接口
            return getInterfaceListLocal();
        } else {// 运行中，从远端拿有变化的接口
            return getChangedInterfaceListRemote();
        }
    }

    /**
     * 从本地拿接口列表
     */
    private List<String> getInterfaceListLocal() {
        File confFile = new File(Conf.getRecommendFlowConfFile());
        File[] files = confFile.listFiles();
        List<String> ifNameList = new ArrayList<>();

        if (MXJudgeUtils.isNotEmpty(files)) {
            String fileNameIt;
            String ifNameIt;
            for (File fileIt : files) {
                fileNameIt = fileIt.getName();
                ifNameIt = fileNameIt.substring(0, fileNameIt.lastIndexOf("."));
                ifNameList.add(ifNameIt);
            }
        }

        return ifNameList;
    }

    /**
     * 从远端拿变化的接口
     */
    private List<String> getChangedInterfaceListRemote() {
        String table = DefineTool.RecommendFlow.ConfigTable.InterfaceStatus.NAME;
        String env = MXStringUtils.isNotEmpty(Conf.getSubEnv()) ? Conf.getSubEnv() : Conf.getEnv();
        String queryFormat = DefineTool.RecommendFlow.ConfigTable.InterfaceStatus.queryFormat(env);
        String column = DefineTool.RecommendFlow.ConfigTable.InterfaceStatus.COLUMN_INTERFACE;
        return MXDataSource.cassandra().getRowsOfColumn(queryFormat, table, column);
    }

    /**
     * 从远端拿配置信息
     */
    private JSONObject getFlowConfig(String interfaceName) {
        String table = DefineTool.RecommendFlow.ConfigTable.FlowConf.NAME;
        String env = MXStringUtils.isNotEmpty(Conf.getSubEnv()) ? Conf.getSubEnv() : Conf.getEnv();
        String queryFormat = DefineTool.RecommendFlow.ConfigTable.FlowConf.queryFormat(env, interfaceName);
        // 目前使用该字段存储所有的内容
        String column = DefineTool.RecommendFlow.ConfigTable.FlowConf.COLUMN_FLOWS;
        String value = MXDataSource.cassandra().getString(queryFormat, table, column);

        return JSONObject.parseObject(value);
    }

    /**
     * 解析配置内容
     */
    private boolean parseFlowConfig(String interfaceName, JSONObject flowConfig) {
        if (MXJudgeUtils.isEmpty(flowConfig)) {
            return false;
        }

        boolean ret = true;

        try {
            // 解析接口小流量配置
            Map<String, RecommendFlow> flowMapIf = new RecommendFlowParserIf(interfaceName).parse(flowConfig);

            // 构建区间到 flow 的映射
            Map<Integer, RecommendFlow> rangeSlotToFlowMap = RecommendFlowParserV2.buildRangeToFlowMap(interfaceName, flowMapIf);

            // 检查已有组件，创建新的组件实例
            checkAndInitConfComponent();

            // 最后一步，将该接口下的所有映射加入到总映射表
            if (MXJudgeUtils.isNotEmpty(rangeSlotToFlowMap)) {
                rangeToFlowMap.put(interfaceName, rangeSlotToFlowMap);
            }
            if (MXJudgeUtils.isNotEmpty(flowMapIf)) {
                recommendFlowMap.putAll(flowMapIf);
            }
        } catch (Exception e) {
            LogTool.reportError(DefineTool.ErrorEnum.GENERAL, logger, e);
            ret = false;
        }

        // 本次解析完成，清空组件源
        ComponentDataSource.INSTANCE.clear();

        return ret;
    }

    /**
     * 检查已有组件，创建新的组件实例
     */
    private void checkAndInitConfComponent() throws Exception {
        // 调用各组件管理器创建自己的可配置组件
        for (IStreamComponentManager manager : managers) {
            manager.check();
        }
    }

    /**
     * 解析推荐流
     *
     * @param filePath 配置文件路径
     */
    @Deprecated
    private void parseRecommendFlow(String filePath) {
        File confFile = new File(filePath);

        if (confFile.isDirectory()) {
            File[] files = confFile.listFiles();
            if (MXJudgeUtils.isNotEmpty(files)) {
                for (File fileIt : files) {
                    parseRecommendFlow(fileIt.getPath());
                }
            }
        } else {
            RecommendFlowParser.parseRecommendFlow(filePath, recommendFlowMap);
        }
    }

    /**
     * 解析推荐流 V2
     * 因为文件格式变了，使用新版解析器，并提供报错
     */
    @Deprecated
    private void parseRecommendFlowV2() {
        RecommendFlowParserV2.parseRecommendFlow(Conf.getRecommendFlowConfFile(), recommendFlowMap, rangeToFlowMap);
    }

    /**
     * 根据推荐流的name获取对应的推荐计算配置
     *
     * @return rf
     */
    @Trace(dispatcher = true)
    public RecommendFlow getRecommendFlowByInterfaceName(BaseDataCollection dc, String interfaceName, String userId) {
        if (null == interfaceName || null == userId) {
            return null;
        }

        dc.client.user.userSmallFlowString = userId + "_" + interfaceName;

        // TODO 暂时将分组规则定为只用userId, 数据流打通后, 再切回上面的逻辑
        dc.client.user.userSmallFlowString = userId;

        dc.client.user.userSmallFlowCode = Math.abs(dc.client.user.userSmallFlowString.hashCode() + 1) % SMALL_FLOW_NAME_BASE_NUMBER;
        logger.debug("[userId=" + userId + " hashcode=" + userId.hashCode() + " r=" + dc.client.user.userSmallFlowCode + "]");

        // 从接口槽位中拿推荐流
        Map<Integer, RecommendFlow> rangeToFlowMapIf = rangeToFlowMap.get(interfaceName);
        if (MXJudgeUtils.isNotEmpty(rangeToFlowMapIf)) {
            return rangeToFlowMapIf.get(dc.client.user.userSmallFlowCode);
        }

        // 该接口没有映射表，报错
        String errMsg = "Range-to-Flow map not found for interface " + interfaceName;
        NewRelic.noticeError(errMsg);
        LogTool.reportError(DefineTool.ErrorEnum.GENERAL, logger, new Exception(errMsg));

        return null;
    }

    /**
     * 根据Debug模式uuid绑定关系获取flow
     *
     * @param dc
     * @param interfaceName
     * @param uuId
     * @return
     */
    @Trace(dispatcher = true)
    public RecommendFlow getRecoFlowByUuId(BaseDataCollection dc, String interfaceName, String uuId) {
        if (MXStringUtils.isBlank(interfaceName) || MXStringUtils.isBlank(uuId)) {
            return null;
        }

        ElasticCacheSource elasticCacheSource = MXDataSource.redis();
        String flowCodeStr = elasticCacheSource.getFlowByUuid(dc);
        if (MXStringUtils.isBlank(flowCodeStr)) {
            return null;
        }
        int flowCode = Integer.parseInt(flowCodeStr);
        dc.client.user.userSmallFlowCode = flowCode;

        // 从接口槽位中拿推荐流
        Map<Integer, RecommendFlow> rangeToFlowMapIf = rangeToFlowMap.get(interfaceName);
        if (MXJudgeUtils.isNotEmpty(rangeToFlowMapIf)) {
            return rangeToFlowMapIf.get(flowCode);
        }

        return null;
    }

    /**
     * 是否使用本地配置（而不是从远端拉取）
     */
    public boolean isUseLocalConfig() {
        return useLocalConfig;
    }
}
