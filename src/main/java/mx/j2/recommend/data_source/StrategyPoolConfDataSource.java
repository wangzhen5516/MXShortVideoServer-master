package mx.j2.recommend.data_source;

import mx.j2.recommend.conf.Conf;
import mx.j2.recommend.pool_conf.StrategyPoolConf;
import mx.j2.recommend.pool_conf.StrategyPoolConfParser;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;

/**
 * @author ：zhongrenli
 * @date ：Created in 9:26 下午 2021/2/1
 */
public class StrategyPoolConfDataSource extends BaseDataSource{
    private static final Logger logger = LogManager.getLogger(StrategyPoolConfDataSource.class);
    private StrategyPoolConf DEFAULT = null;

    private Map<String, StrategyPoolConf> highStrategyPoolConfMap;

    private Map<String, StrategyPoolConf> lowStrategyPoolConfMap;

    private Set<String> poolSet;

    public StrategyPoolConfDataSource() {
        init();
        logger.info("{\"dataSourceInfo\":\"[StrategyPoolConfDataSource init successfully]\"}");
    }

    private void init() {
        poolSet = new HashSet<>();
        highStrategyPoolConfMap = new LinkedHashMap<>(16);
        lowStrategyPoolConfMap = new LinkedHashMap<>(16);
        Map<String, StrategyPoolConf> highTemp = new HashMap<>(16);
        Map<String, StrategyPoolConf> lowTemp = new HashMap<>(16);
        StrategyPoolConfParser.parse(Conf.getStrategyPoolConf(), highTemp, lowTemp);
        sort(highTemp, highStrategyPoolConfMap);
        lowStrategyPoolConfMap.putAll(lowTemp);
        poolSet.addAll(highStrategyPoolConfMap.keySet());
        poolSet.addAll(lowStrategyPoolConfMap.keySet());
    }

    private void sort(Map<String, StrategyPoolConf> temp, Map<String, StrategyPoolConf> map) {
        List<Map.Entry<String, StrategyPoolConf>> list = new ArrayList<>(temp.entrySet());
        list.sort((o1, o2) -> o2.getValue().priority - o1.getValue().priority);
        for (Map.Entry<String, StrategyPoolConf> entry : list) {
            map.put(entry.getKey(), entry.getValue());
        }
    }

    public Set<String> getPoolSet() {
        return poolSet;
    }

    public StrategyPoolConf getStrategyPoolConf(String poolIndex) {
        if (highStrategyPoolConfMap.containsKey(poolIndex)) {
            return highStrategyPoolConfMap.get(poolIndex);
        }
        return lowStrategyPoolConfMap.getOrDefault(poolIndex, DEFAULT);
    }
}
