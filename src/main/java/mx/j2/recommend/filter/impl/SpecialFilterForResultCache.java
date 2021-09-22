package mx.j2.recommend.filter.impl;

import com.alibaba.fastjson.JSONObject;
import com.newrelic.api.agent.Trace;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.thrift.Result;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXJudgeUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

import static mx.j2.recommend.util.BaseMagicValueEnum.METADATA_ID;
import static mx.j2.recommend.util.BaseMagicValueEnum.STATUS;

/**
 * @ Author     ：xuejian.zhang
 * @ Date       ：Created in 下午6:53 2020/8/14
 * @ Description：专门为走缓存回来的cacheList做过滤的过滤器, 目前只处理上下线状态, 后续有需求, 可以往里面加
 */
public class SpecialFilterForResultCache extends BaseFilter<BaseDataCollection> {
    private static Logger logger = LogManager.getLogger(SpecialFilterForResultCache.class);

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
        List<String> idList = new ArrayList<>();
        List<String> delIdList = new ArrayList<>();
        List<Result> delList = new ArrayList<>();
        for (Result r : baseDc.cachedResultList) {
            idList.add(r.shortVideo.id);
        }

        List<BaseDocument> documents = MXDataSource.details().get(idList, "");

        for (BaseDocument object : documents) {
            if (isFilted(object)) {
                delIdList.add(object.getId());
            }
        }
        for (Result r : baseDc.cachedResultList) {
            if (delIdList.contains(r.shortVideo.id)) {
                delList.add(r);
            }
        }

        baseDc.cachedResultList.removeAll(delList);
        if (MXJudgeUtils.isNotEmpty(delList)) {
            baseDc.debug.deletedRecordMap.put(this.getName(), delList.size());
        }
        baseDc.debug.deletedIdRecordMap.put(this.getName(), delIdList);

    }

    @Override
    public boolean isFilted(BaseDocument doc, BaseDataCollection dc) {
        return false;
    }

    public boolean isFilted(BaseDocument obj) {
        // 因为需要收集id, 所以为空的情况或没有id的情况, 不用过滤
        if (null == obj) {
            return false;
        }

        if (MXJudgeUtils.isEmpty(obj.getId())) {
            return false;
        }

        if (obj.getStatus() != DefineTool.OnlineStatusesEnum.ONLINE.getIndex()) {
            return true;
        }

        return false;
    }

}
