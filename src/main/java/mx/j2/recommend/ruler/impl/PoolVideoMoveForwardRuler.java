package mx.j2.recommend.ruler.impl;

import mx.j2.recommend.data_model.data_collection.FeedDataCollection;
import mx.j2.recommend.thrift.Result;
import mx.j2.recommend.util.MXJudgeUtils;
import mx.j2.recommend.util.MXStringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;

/**
 * 支持配置多个池子视频的置顶，配置方法如下：
 * PoolVideoMoveForwardRuler{poolPriority:[12|13]}
 * 配置时，如果有多个池子级别，请一定注意使用"|"进行分割，如果用","解析flow时会发生错误
 */
public class PoolVideoMoveForwardRuler extends BaseRuler<FeedDataCollection> {
    private final Logger log = LogManager.getLogger(PoolVideoMoveForwardRuler.class);

    private final String KEY_PRIORITY = "poolPriority";
    private final String START = "[";
    private final String END = "]";
    private final int LIMITED_LENGTH = 2;

    @Override
    public boolean skip(FeedDataCollection dc) {
        return MXJudgeUtils.isEmpty(dc.data.result.resultList);
    }

    @Override
    public void rule(FeedDataCollection dc) {
        String poolPriority = config.getString(KEY_PRIORITY).replaceAll(" ", "");
        Set<Integer> prioritySet = new HashSet<>();
        if (poolPriority.startsWith(START) && poolPriority.endsWith(END) && poolPriority.length() > LIMITED_LENGTH) {
            String[] rawPriority = MXStringUtils.split(poolPriority.substring(1, poolPriority.length() - 1), "|");
            try {
                for (String raw : rawPriority) {
                    int priority = Integer.parseInt(raw);
                    prioritySet.add(priority);
                }
            } catch (Exception e) {
                log.error("PoolVideoMoveForwardRuler parseInt failed: " + e);
                return;
            }
        } else {
            log.error("PoolVideoMoveForwardRuler config error, wrong config format");
            return;
        }

        List<Result> forwardResult = new ArrayList<>();
        dc.data.result.resultList.forEach(result -> {
            if (result.internalUse != null && prioritySet.contains(result.internalUse.getPoolPriority())) {
                forwardResult.add(result);
            }
        });

        dc.data.result.resultList.removeAll(forwardResult);
        dc.data.result.resultList.addAll(0, forwardResult);
    }

    @Override
    public void registerConfig(Map<String, Class> outConfMap) {
        outConfMap.put(KEY_PRIORITY, String.class);
    }
}
