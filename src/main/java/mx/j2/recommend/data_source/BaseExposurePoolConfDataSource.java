package mx.j2.recommend.data_source;

import mx.j2.recommend.pool_conf.ExposurePoolConf;
import mx.j2.recommend.pool_conf.ExposurePoolConfParser;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.FileTool;
import mx.j2.recommend.util.LogTool;
import mx.j2.recommend.util.MXJudgeUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

/**
 * 曝光池配置数据源基类
 */
public abstract class BaseExposurePoolConfDataSource extends BaseDataSource {
    private final Logger logger = LogManager.getLogger(BaseExposurePoolConfDataSource.class);

    /**
     * <smallflow/base, poolconf>
     */
    private Map<String, ExposurePoolConf> flowToPoolConfMap = new HashMap<>();

    public BaseExposurePoolConfDataSource() {
        init();
    }

    private void init() {
        if (parseConf()) {
            logger.info("{\"DataSourceInfo\":\"[" + getName() + " init successfully]\"}");
        }
    }

    /**
     * 解析本地配置文件
     *
     * @return true for parse successfully and false for otherwise
     */
    private boolean parseConf() {
        String confContent = FileTool.readContent(getConfPath());

        if (MXJudgeUtils.isNotEmpty(confContent)) {
            return parseConfContent(confContent);
        } else {
            logger.error("Failed to parse " + getConfPath() + " because empty content.");
            return false;
        }
    }

    /**
     * 解析配置内容（JSON 字符串格式）
     */
    private boolean parseConfContent(String confContent) {
        try {
            ExposurePoolConfParser.parse(confContent, flowToPoolConfMap);
            return MXJudgeUtils.isNotEmpty(flowToPoolConfMap);
        } catch (Exception e) {
            LogTool.reportError(DefineTool.ErrorEnum.GENERAL, logger, e);
            return false;
        }
    }

    /**
     * 根据池子名称返回池子配置
     */
    public ExposurePoolConf get(String flow) {
        return flowToPoolConfMap.getOrDefault(flow, flowToPoolConfMap.get("base"));
    }

    /**
     * 所有的小流量配置
     */
    public Map<String, ExposurePoolConf> all() {
        return flowToPoolConfMap;
    }

    /**
     * 配置文件路径
     */
    abstract String getConfPath();
}
