package mx.j2.recommend.prefilter.impl;

import com.alibaba.fastjson.JSON;
import com.newrelic.api.agent.Trace;
import mx.j2.recommend.component.configurable.base.BaseConfigurablePreFilter;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.pool_conf.PoolConf;
import mx.j2.recommend.util.MXJudgeUtils;
import mx.j2.recommend.util.MXStringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;

/**
 * @author liangjie.feng
 * @version 1.0
 * @date 2021/5/11 下午3:13
 * @description
 */
public abstract class BasePreFilter<D extends BaseDataCollection> extends BaseConfigurablePreFilter<BaseDocument, D> {

    private static final Logger log = LogManager.getLogger(BasePreFilter.class);

    @Override
    public void doWork(D dc) {

    }

    @Override
    public void preFilter(D dc) {

    }

    public void doWork(Map<String, List<String>> poolToIdListMap, D dc, String recallName) {
        prepare(dc);
        filter(poolToIdListMap, dc, recallName);
    }

    @Trace(dispatcher = true)
    public void filter(Map<String, List<String>> poolToIdListMap, D dc, String recallName) {
        if (MXJudgeUtils.isNotEmpty(poolToIdListMap)) {
            for (Map.Entry<String, List<String>> entry : poolToIdListMap.entrySet()) {
                List<String> idList = entry.getValue();

                PoolConf pc = dc.poolConfMap.get(entry.getKey());
                if (pc == null) {//不应该出现
                    continue;
                }
                if (MXJudgeUtils.isNotEmpty(idList) && !pc.ignoreFilter.contains(this.getFullName())) {// ignore 配置里配的filter
                    filterOneList(idList, dc, String.format("_%s", entry.getKey()), recallName);
                }
            }
        }
    }

    private void filterOneList(List<String> ids, D dc, String suffix, String recallName) {
        if (MXJudgeUtils.isEmpty(ids)) {
            return;
        }

        List<String> idsLimit = new ArrayList<>();
        int num = 0, deleteNum = 0;
        for (String id : ids) {
            if (!isFilted(id, dc)) {
                if (num >= 300) {
                    break;
                }
                idsLimit.add(id);
                num++;
            } else {
                deleteNum++;
            }
        }
        ids.clear();
        ids.addAll(idsLimit);

        dc.appendToDeletedRecord(deleteNum, getFilterFlagText() + suffix);
    }

    /**
     * 获取过滤器唯一标识
     */
    private String getFilterFlagText() {
        String filterFlag = this.getName();

        // 如果有配置，再追加 test 项以互相区分，因为类名可能都是一样的
        if (config != null && config.getTest() != null) {
            filterFlag += MXStringUtils.toSuffix(config.getTest().getClass().getSimpleName());
        }

        return filterFlag;
    }

    /**
     * 子类必须实现的方法，判断一个doc是否需要被过滤掉
     * <p>
     * V2:
     * 修改为默认方法，默认执行可配置的文档过滤逻辑；
     * 非可配置过滤器继续重写该方法即可。
     *
     * @param id
     * @return
     */
    public boolean isFilted(String id, D dc) {
        return false;
    }
}
