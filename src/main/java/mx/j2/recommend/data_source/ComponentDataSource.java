package mx.j2.recommend.data_source;

import mx.j2.recommend.component.stream.base.IStreamComponent;
import mx.j2.recommend.util.MXJudgeUtils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * 可配置组件数据源
 *
 * @description 解析 flow 配置时，先将 flow 配置中的可配置组件暂存起来
 */
public enum ComponentDataSource {
    INSTANCE;

    /**
     * 配置字符串集合
     * <组件类型，可配置组件列表>
     */
    private Map<IStreamComponent.TypeEnum, Set<String>> componentMap = new HashMap<>();

    /**
     * 添加一个可配置组件
     *
     * @param type      组件类型
     * @param configStr 配置源串，包含配置信息
     */
    public void add(IStreamComponent.TypeEnum type, String configStr) {
        if (MXJudgeUtils.isBlank(configStr)) {
            return;
        }

        Set<String> components = componentMap.computeIfAbsent(type, s -> new HashSet<>());
        components.add(configStr);
    }

    /**
     * 获取某个类型的所有可配置组件
     */
    public Set<String> getComponents(IStreamComponent.TypeEnum type) {
        return componentMap.get(type);
    }

    /**
     * 使用完清空释放内存
     */
    public void clear() {
        componentMap.clear();
    }
}
