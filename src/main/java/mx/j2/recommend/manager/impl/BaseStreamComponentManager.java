package mx.j2.recommend.manager.impl;

import mx.j2.recommend.component.stream.base.BaseStreamComponent;
import mx.j2.recommend.component.stream.base.IStreamComponent;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_source.ComponentDataSource;
import mx.j2.recommend.manager.IStreamComponentManager;
import mx.j2.recommend.util.MXJudgeUtils;

import java.util.Set;

import static mx.j2.recommend.util.DefineTool.Cache.CacheStatus.IgnoreAll;

/**
 * @author zhendong.wang
 * @version 1.0
 * @date 2021/4/19 下午5:52
 * @description 流组件管理器基类
 */
abstract class BaseStreamComponentManager<T extends BaseStreamComponent>
        extends BaseComponentManager<T>
        implements IStreamComponentManager<T> {

    /**
     * 检查组件存在性
     */
    @Override
    public void check() throws Exception {
        IStreamComponent.TypeEnum type = getComponentType();
        Set<String> components = ComponentDataSource.INSTANCE.getComponents(type);
        if (MXJudgeUtils.isEmpty(components)) {
            return;
        }

        for (String componentStrIt : components) {
            if (!componentMap.containsKey(componentStrIt)) {
                throw new ClassNotFoundException(componentStrIt);
            }
        }
    }

    @Override
    public boolean skip(BaseDataCollection dc) {
        return IgnoreAll == dc.util.cacheStatus;
    }

    @Override
    public String getComponentInterfacePath() {
        return getComponentType().getStreamComponentIfPath();
    }
}
