package mx.j2.recommend.statistic_conf;

import lombok.Data;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author ：zhongrenli
 * @date ：Created in 2:49 下午 2021/4/17
 */
@Data
public class StatisticConf {

    private String statisticName;

    private String suffix;

    private int priority;

    private Map<String, Double> preConditionGreaterThan;

    private Map<String, Double> baseConditionGreaterThan;

    private Map<String, Double> preConditionLessThan;

    private Map<String, Double> baseConditionLessThan;

    private List<String> exclude;

    private List<String> indexList;

    private List<String> indexStringList;

    private Map<String, StatisticConf> smallFlowConf;

    private String description;

    public void putToPreConditionGreaterThan(String k, double v) {
        if (null == preConditionGreaterThan) {
            preConditionGreaterThan = new HashMap<>();
        }
        preConditionGreaterThan.put(k, v);
    }

    public void putToPreConditionLessThan(String k, double v) {
        if (null == preConditionLessThan) {
            preConditionLessThan = new HashMap<>();
        }
        preConditionLessThan.put(k, v);
    }

    public void putToBaseConditionGreaterThan(String k, double v) {
        if (null == baseConditionGreaterThan) {
            baseConditionGreaterThan = new HashMap<>();
        }
        baseConditionGreaterThan.put(k, v);
    }

    public void putToBaseConditionLessThan(String k, double v) {
        if (null == baseConditionLessThan) {
            baseConditionLessThan = new HashMap<>();
        }
        baseConditionLessThan.put(k, v);
    }

    public void putToSmallFlowConf(String k, StatisticConf v) {
        if (null == smallFlowConf) {
            smallFlowConf = new HashMap<>();
        }
        smallFlowConf.put(k, v);
    }

}
