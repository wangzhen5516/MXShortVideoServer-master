package mx.j2.recommend.pool_conf;

import com.alibaba.fastjson.JSONArray;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

import java.util.*;

/**
 * @author ：zhongrenli
 * @date ：Created in 5:10 下午 2021/2/1
 */
public class StrategyPoolConf {
    public String poolIndexPrefix;

    public double basePercentage;

    public int poolRecallSize;

    public int priority;

    public JSONArray poolLevel;

    public String poolDescription;

    public Set<String> excludeSmallFlowList;

    public Map<String, InternalSmallFlow> smallFlowMap;

    public static class InternalSmallFlow {
        public String smallFlowName;
        public String poolIndexPrefix;
        public double percentage;
    }

    public StrategyPoolConf() {
        poolIndexPrefix = "";
        basePercentage =0.0;
        priority = 0;
        poolDescription = "";
        excludeSmallFlowList = new HashSet<>();
        smallFlowMap = new HashMap<>();
        poolLevel = new JSONArray();
    }

    public StrategyPoolConf(StrategyPoolConf other) {
        if (StringUtils.isNotEmpty(other.poolIndexPrefix)) {
            this.poolIndexPrefix = other.poolIndexPrefix;
        }

        if (StringUtils.isNotEmpty(other.poolDescription)) {
            this.poolDescription = other.poolDescription;
        }

        if (CollectionUtils.isNotEmpty(other.excludeSmallFlowList)) {
            this.excludeSmallFlowList.addAll(other.excludeSmallFlowList);
        }

        if (null != other.smallFlowMap && !other.smallFlowMap.isEmpty()) {
            this.smallFlowMap.putAll(other.smallFlowMap);
        }

        if (null != other.poolLevel) {
            this.poolLevel = other.poolLevel;
        }

        this.basePercentage = other.basePercentage;
        this.priority = other.priority;
    }

    public StrategyPoolConf deepCopy() {
        return new StrategyPoolConf(this);
    }

    @Override
    public String toString() {
        return "StrategyPoolConf{" +
                "poolIndexPrefix='" + poolIndexPrefix + '\'' +
                ", basePercentage=" + basePercentage +
                ", poolRecallSize=" + poolRecallSize +
                ", priority=" + priority +
                ", poolLevel=" + poolLevel +
                ", poolDescription='" + poolDescription + '\'' +
                ", excludeSmallFlowList=" + excludeSmallFlowList +
                ", smallFlowMap=" + smallFlowMap +
                '}';
    }
}
