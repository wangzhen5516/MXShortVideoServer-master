package mx.j2.recommend.filter.impl;

import com.newrelic.api.agent.Trace;
import mx.j2.recommend.component.configurable.base.BaseConfigurableFilter;
import mx.j2.recommend.data_model.UserProfile;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.data_collection.info.MXEntityDebugInfo;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.pool_conf.PoolConf;
import mx.j2.recommend.util.MXJudgeUtils;
import mx.j2.recommend.util.MXStringUtils;
import mx.j2.recommend.util.annotation.CollectionList;
import org.apache.commons.collections.CollectionUtils;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * 基础过滤类
 *
 * @author zhuowei
 * <p>
 * TODO: skip 要放开；文档要用泛型
 */
public abstract class BaseFilter<D extends BaseDataCollection> extends BaseConfigurableFilter<BaseDocument, D> {
    @Override
    @Trace(dispatcher = true)
    public void filter(D dc) {
        // 过滤特殊数据
        filterSpecial(dc);

        // 过滤常规列表
        filterLists(dc);

        // 过滤统一管理的召回结果
        filterResult(dc);
    }

    private void filterSpecial(D dc) {
        if (MXJudgeUtils.isNotEmpty(dc.poolToDocumentListMap)) {
            for (Map.Entry<String, List<BaseDocument>> entry : dc.poolToDocumentListMap.entrySet()) {
                List<BaseDocument> docList = entry.getValue();

                PoolConf pc = dc.poolConfMap.get(entry.getKey());
                if (pc == null) {//不应该出现
                    continue;
                }
                if (MXJudgeUtils.isNotEmpty(docList) && !pc.ignoreFilter.contains(this.getFullName())) {// ignore 配置里配的filter
                    filterOneList(docList, dc, String.format("_%s", entry.getKey()));
                }
            }
        }

        if (null != dc.strategyPoolToDocumentListMap && !dc.strategyPoolToDocumentListMap.isEmpty()) {
            for (Map.Entry<String, List<BaseDocument>> entry : dc.strategyPoolToDocumentListMap.entrySet()) {
                List<BaseDocument> docList = entry.getValue();
                if (CollectionUtils.isNotEmpty(docList)) {
                    filterOneList(docList, dc, String.format("_LONG_%s", entry.getKey()));
                }
            }
        }

        if (null != dc.realTimeStrategyPoolToDocumentListMap && !dc.realTimeStrategyPoolToDocumentListMap.isEmpty()) {
            for (Map.Entry<String, List<BaseDocument>> entry : dc.realTimeStrategyPoolToDocumentListMap.entrySet()) {
                List<BaseDocument> docList = entry.getValue();
                if (CollectionUtils.isNotEmpty(docList)) {
                    filterOneList(docList, dc, String.format("_SHORT_%s", entry.getKey()));
                }
            }
        }

        if (MXJudgeUtils.isNotEmpty(dc.userProfileTagMapNew)) {
            for (Map.Entry<UserProfile.Tag, List<BaseDocument>> entry : dc.userProfileTagMapNew.entrySet()) {
                List<BaseDocument> docList = entry.getValue();
                if (MXJudgeUtils.isNotEmpty(docList)) {
                    filterOneList(docList, dc, String.format("_%s", entry.getKey().name));
                }
            }
        }


        if (MXJudgeUtils.isNotEmpty(dc.userProfileTagMap)) {
            for (Map.Entry<UserProfile.Tag, List<BaseDocument>> entry : dc.userProfileTagMap.entrySet()) {
                List<BaseDocument> docList = entry.getValue();
                if (MXJudgeUtils.isNotEmpty(docList)) {
                    filterOneList(docList, dc, String.format("_%s", entry.getKey().name));
                }
            }
        }

        if (MXJudgeUtils.isNotEmpty(dc.preferredPublisherVideoList)) {
            filterOneList(dc.preferredPublisherVideoList, dc, "_preferredPublisherVideo");
        }

    }

    private void filterLists(D dc) {
        //反射使用CollectionList annotation来调用
        Field[] fields = BaseDataCollection.class.getDeclaredFields();
        for (Field field : fields) {
            if (field.isAnnotationPresent(CollectionList.class)) {
                CollectionList annotation = field.getAnnotation(CollectionList.class);
                String suffix = annotation.suffix();
                Object o;
                try {
                    o = field.get(dc);
                    if (o instanceof List) {
                        List<BaseDocument> collectionList = (List<BaseDocument>) o;
                        filterOneList(collectionList, dc, suffix);
                    }
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    /**
     * 过滤统一管理的召回结果
     */
    private void filterResult(D dc) {
        for (Map.Entry<String, Object> entry : dc.data.recall.resultMap.entrySet()) {
            Object value = entry.getValue();
            if (value instanceof List) {// 召回结果是普通列表
                filterOneList((List<BaseDocument>) value, dc, MXStringUtils.toSuffix(entry.getKey()));
            } else if (value instanceof Map) {// 召回结果是 Map
                Map<?, List<BaseDocument>> mapValue = (Map<?, List<BaseDocument>>) value;
                for (Map.Entry<?, List<BaseDocument>> mapEntry : mapValue.entrySet()) {
                    filterOneList(mapEntry.getValue(), dc, MXStringUtils.toSuffix(mapEntry.getKey().toString()));
                }
            } else {// 不支持的类型
                throw new RuntimeException("Not support recall result type: " + value.getClass());
            }
        }
    }

    private void filterOneList(List<BaseDocument> documents, D dc, String suffix) {
        if (MXJudgeUtils.isEmpty(documents)) {
            return;
        }
        Collection<BaseDocument> deleted = new LinkedList<>();
        for (BaseDocument doc : documents) {
            if (isFilted(doc, dc)) {
                deleted.add(doc);

                MXEntityDebugInfo debugInfo = dc.debug.getDebugInfoByEntityId(doc.id);
                String recallName = debugInfo.recall.name;

                dc.recallFilterCount.add(recallName);
            }
        }

        documents.removeAll(deleted);
        dc.appendToDeletedRecord(deleted.size(), getFilterFlagText() + suffix);
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

    @Override
    public void doWork(D dc) {
        filter(dc);
    }

    /**
     * 子类必须实现的方法，判断一个doc是否需要被过滤掉
     * <p>
     * V2:
     * 修改为默认方法，默认执行可配置的文档过滤逻辑；
     * 非可配置过滤器继续重写该方法即可。
     *
     * @param doc
     * @return
     */
    public boolean isFilted(BaseDocument doc, D dc) {
        return isFiltered(doc, dc);
    }
}
