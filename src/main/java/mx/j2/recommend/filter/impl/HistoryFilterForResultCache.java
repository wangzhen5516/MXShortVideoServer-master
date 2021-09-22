package mx.j2.recommend.filter.impl;

import com.newrelic.api.agent.Trace;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_source.ElasticCacheSource;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.thrift.Result;
import mx.j2.recommend.util.MXJudgeUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * @ Author     ：xuejian.zhang
 * @ Date       ：Created in 下午6:53 2020/8/14
 * @ Description：专门为走缓存回来的cacheList做过滤的过滤器, 过滤掉看过的视频
 */
public class HistoryFilterForResultCache extends BaseFilter<BaseDataCollection> {
    private static Logger logger = LogManager.getLogger(HistoryFilterForResultCache.class);

    @Override
    public boolean skip(BaseDataCollection baseDc) {
        if (baseDc == null || baseDc.req == null || baseDc.req.getUserInfo() == null) {
            logger.error("get a null UserInfo");
            return true;
        }

        if (MXJudgeUtils.isEmpty(baseDc.cachedResultList)) {
            return true;
        }

        return false;
    }

    @Override
    @Trace(dispatcher = true)
    public void filter(BaseDataCollection baseDc) {
        List<Result> delList = new ArrayList<>();
        List<String> delIdList = new ArrayList<>();

        for (Result r : baseDc.cachedResultList) {
            if (baseDc.historyIdList.contains(r.shortVideo.id)) {
                delList.add(r);
                delIdList.add(r.shortVideo.id);
            }
        }

        baseDc.cachedResultList.removeAll(delList);
        if (MXJudgeUtils.isNotEmpty(delList)) {
            baseDc.debug.deletedRecordMap.put(this.getName(), delList.size());
        }
        baseDc.debug.deletedIdRecordMap.put(this.getName(), delIdList);
    }

    private void loadHistory(BaseDataCollection baseDc) {
        ElasticCacheSource elasticCacheSource = MXDataSource.redis();
        elasticCacheSource.getUserRecommendHistoryList(baseDc);
    }

    @Override
    public boolean prepare(BaseDataCollection dc) {
        loadHistory(dc);
        return true;
    }

    @Override
    public boolean isFilted(BaseDocument doc, BaseDataCollection dc) {
        return false;
    }

}
