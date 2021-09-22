package mx.j2.recommend.component.configurable.base;

import mx.j2.recommend.component.configurable.ConfigValuePair;
import mx.j2.recommend.component.configurable.config.RecallConfig;
import mx.j2.recommend.component.list.skip.ISkip;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.data_collection.info.MXDebugInfo;
import mx.j2.recommend.data_model.data_collection.info.MXEntityDebugInfo;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.recall.IRecall;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXJudgeUtils;

import java.util.List;
import java.util.Map;

/**
 * 可配置的召回器基类
 *
 * @param <T> DC 类型
 * @see BaseDataCollection
 */
public abstract class BaseConfigurableRecall<T extends BaseDataCollection>
        extends BaseConfigurableStreamComponent<T, RecallConfig<T>>
        implements IRecall<T>, BaseDataCollection.IResult {

    /**
     * 默认配置项
     *
     * @param outConfMap <键，值类型>
     */
    @Override
    public void registerConfig(Map<String, Class> outConfMap) {
        outConfMap.put(RecallConfig.KEY_SKIP, ISkip.class);
        outConfMap.put(RecallConfig.KEY_RESULT, String.class);
    }

    @Override
    public RecallConfig<T> newConfig(Map<String, ConfigValuePair> confMap) throws Exception {
        return new RecallConfig<>(confMap);
    }

    @Override
    public String getResultKey() {
        return config != null ? config.getResultKey() : "";
    }

    /**
     * 召回器唯一标识，标识数据是从哪个召回器实例召回的
     */
    @Override
    public String getId() {
        return DefineTool.toKey(super.getId(), getDataId());
    }

    /**
     * 获取召回数据唯一标识，即数据是从哪来的
     * 完全由子类定义
     */
    protected String getDataId() {
        return "";
    }

    protected int getSize() {
        return config != null ? config.getSize() : 0;
    }

    protected String getEsIndex() {
        return config.getEsIndex();
    }

    /**
     * 添加一个结果
     */
    protected void addResult(T dc, List<BaseDocument> result) {
        fillDebugInfo(dc, result);
        dc.addResult(getResultKey(), result);
    }

    /**
     * 设置（覆盖）一个结果
     *
     * @param resultObject 目前只有 List 和 Map 两种结构
     */
    protected void setResult(T dc, Object resultObject) {
        fillDebugInfo(dc, resultObject);
        dc.setResult(getResultKey(), resultObject);
    }

    /**
     * 设置召回信息
     */
    protected void setResultInfo(T dc, int size, DefineTool.RecallFrom from) {
        dc.syncSearchResultSizeMap.put(getName(), size);
        dc.resultFromMap.put(getName(), from.getName());
    }

    /**
     * 为召回结果填充调试信息
     */
    private void fillDebugInfo(T dc, Object object) {
        if (object instanceof List) {
            List<BaseDocument> list = (List<BaseDocument>) object;
            if (MXJudgeUtils.isNotEmpty(list)) {
                for (BaseDocument document : list) {
                    fillEntityDebugInfo(dc, document);
                }
            }
        } else if (object instanceof Map) {
            Map<String, List<BaseDocument>> map = (Map<String, List<BaseDocument>>) object;
            if (MXJudgeUtils.isNotEmpty(map)) {
                for (List<BaseDocument> list : map.values()) {
                    fillDebugInfo(dc, list);
                }
            }
        }
    }

    /**
     * 为实体写入调试信息
     */
    private void fillEntityDebugInfo(T dc, BaseDocument document) {
        MXDebugInfo debugInfo = dc.debug;
        String recallName = getName();
        String recallResult = getResultKey();

        // 召回器类的名字
        document.setRecallName(recallName);
        // 配置的拉链的名字
        document.setRecallResultID(recallResult);

        // 调试信息
        MXEntityDebugInfo entityDebugInfo = debugInfo.getDebugInfoByEntityId(document.id);
        entityDebugInfo.recall.name = recallName;
        entityDebugInfo.recall.result = recallResult;
        entityDebugInfo.recall.sourceId = dc.debug.sourceId;
    }
}