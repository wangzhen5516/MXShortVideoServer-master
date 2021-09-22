package mx.j2.recommend.data_model.data_collection.info;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author zhendong.wang
 * @version 1.0
 * @date 2021/6/26 下午12:36
 * @description 调试信息
 */
public class MXDebugInfo extends MXBaseDCInfo {
    /**
     * 实体调试信息
     */
    private Map<String, MXEntityDebugInfo> entityDebugInfoMap;

    /**
     * 记录时间数据。
     */
    public Map<String, Integer> timeRecordMap;

    /**
     * 过滤器删除数据数量记录。
     */
    public Map<String, Integer> deletedRecordMap;

    /**
     * 日志收集所需记录
     */
    public Map<String, Integer> logCollectorMap;

    /**
     * video attach info
     */
    public Map<String, String> attachInfoMap;

    /**
     * 记录哪些id是被那个过滤器删掉的。
     */
    public Map<String, List<String>> deletedIdRecordMap;

    /**
     * 记录debugInfo的Map
     */
    public Map<String, String> debugInfoMap;

    /**
     * 记录结果是根据源Id推荐的
     */
    public String sourceId;

    /**
     * 初始化函数
     */
    public MXDebugInfo() {
        entityDebugInfoMap = new HashMap<>();
        timeRecordMap = new HashMap<>();
        logCollectorMap = new HashMap<>();
        deletedIdRecordMap = new HashMap<>();
        deletedRecordMap = new HashMap<>();
        attachInfoMap = new HashMap<>();
        debugInfoMap = new HashMap<>();
        sourceId = "";
    }

    @Override
    public void clean() {
        entityDebugInfoMap.clear();
        deletedRecordMap.clear();
        timeRecordMap.clear();
        logCollectorMap.clear();
        attachInfoMap.clear();
        deletedIdRecordMap.clear();
        debugInfoMap.clear();
        sourceId = "";
    }

    @Nonnull
    public MXEntityDebugInfo getDebugInfoByEntityId(String id) {
        return entityDebugInfoMap.computeIfAbsent(id, entityId -> new MXEntityDebugInfo());
    }
}
