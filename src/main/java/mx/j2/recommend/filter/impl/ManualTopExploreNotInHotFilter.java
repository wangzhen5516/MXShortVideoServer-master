package mx.j2.recommend.filter.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_source.ElasticCacheSource;
import mx.j2.recommend.data_source.LocalCacheDataSource;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.MXJudgeUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * @author zhongrenli
 */
public class ManualTopExploreNotInHotFilter extends BaseFilter {

    private final static String REDIS_KEY = "manual_top_explore_list";

    private final static int RECALL_SIZE = -1;

    private Map<String, Double> map = new HashMap<>();

    @Override
    public boolean isFilted(BaseDocument doc, BaseDataCollection baseDc) {
        if (null == doc) {
            return true;
        }

        if (MXJudgeUtils.isEmpty(map)) {
            map = getManualTopList(baseDc);
            if (MXJudgeUtils.isEmpty(map)) {
                return false;
            }
        }

        return map.containsKey(doc.id);
    }

    private Map<String, Double> getManualTopList(BaseDataCollection baseDc) {
        String localRedisListCacheKey = String.format("%s_%s_%s_%s", baseDc.req.getInterfaceName(), baseDc.req.getTabId(), REDIS_KEY, "_manual_redis_list");
        LocalCacheDataSource localCacheDataSource = MXDataSource.cache();
        Map<String, Double> manualControlMap = localCacheDataSource.getManualControlRedisListCache(localRedisListCacheKey);

        if (MXJudgeUtils.isEmpty(manualControlMap)) {
            ElasticCacheSource elasticCacheSource = MXDataSource.redis();
            manualControlMap = elasticCacheSource.getManualControltCache(REDIS_KEY, RECALL_SIZE);
        }
        return manualControlMap;
    }
}
