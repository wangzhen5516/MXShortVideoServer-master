package mx.j2.recommend.prefilter.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_source.ElasticCacheSource;
import mx.j2.recommend.manager.MXDataSource;

/**
 * @author liangjie.feng
 * @version 1.0
 * @date 2021/5/12 下午1:54
 * @description
 */
public class UserRecommendHistoryPreFilter extends BasePreFilter {
    private void loadHistory(BaseDataCollection baseDc) {
        ElasticCacheSource elasticCacheSource = MXDataSource.redis();
        elasticCacheSource.getUserRecommendHistoryList(baseDc);
        // the new list
        elasticCacheSource.getUserRecommendHistoryListNew(baseDc);

//        if (CollectionUtils.isEmpty(baseDc.historyIdList)) {
//            InactiveUserHistoryBloomDataSource dataSource = DataSourceManager.INSTANCE.getInactiveUserHistoryBloomDataSource();
//            String userId = BloomUtil.getUserId(baseDc);
//            if (dataSource.exists("user_history_in_bloom", userId)) {
//                StrategyCassandraDataSource strategyCassandraDataSource = DataSourceManager.INSTANCE.getStrategyCassandraDataSource();
//                List<String> idList = strategyCassandraDataSource.getUserHistoryList(userId);
//                if (CollectionUtils.isEmpty(idList)) {
//                    return;
//                }
//                baseDc.historyIdList.addAll(idList);
//                baseDc.isComeFromCassandra = true;
//            }
//        }

        // only top hot list
        elasticCacheSource.getUserRecommendHistoryListOnlyTopHot(baseDc);

        elasticCacheSource.getUserRecommendHistoryListNotTopHot(baseDc);
    }

    @Override
    public boolean prepare(BaseDataCollection dc) {
        loadHistory(dc);
        return true;
    }

    @Override
    public boolean isFilted(String id, BaseDataCollection baseDc) {
        if (baseDc.historyIdList.contains(id)) {
            return true;
        }
        return false;
    }
}
