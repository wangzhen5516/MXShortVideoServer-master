package mx.j2.recommend.manager.impl;

import mx.j2.recommend.component.configurable.ComponentConfig;
import mx.j2.recommend.component.configurable.ConfigurableComponentParser;
import mx.j2.recommend.component.configurable.base.BaseConfigurableStreamComponent;
import mx.j2.recommend.component.stream.base.IStreamComponent;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.interfaces.IParser;
import mx.j2.recommend.data_source.ComponentDataSource;
import mx.j2.recommend.manager.IStreamComponentManager;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.LogTool;
import mx.j2.recommend.util.MXJudgeUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static mx.j2.recommend.util.DefineTool.Cache.CacheStatus.IgnoreAll;

/**
 * @author zhendong.wang
 * @version 1.0
 * @date 2021/4/19 下午5:52
 * @description 可配置流组件管理器基类
 */
abstract class BaseConfStreamComponentManager<T extends BaseConfigurableStreamComponent>
        extends BaseComponentManager<T>// 本质是组件管理器
        implements IStreamComponentManager<T> {// 流处理功能接口

    @Override
    public void onDataSourcePrepared() {
        // 如果万一恢复了使用本地配置，还得在此处创建可配置组件
        if (MXDataSource.flow().isUseLocalConfig()) {
            try {
                check();
            } catch (Exception e) {
                LogTool.reportError(DefineTool.ErrorEnum.FATAL, null, e);
            }
        }
    }

    /**
     * 自检，这里包括创建配置组件和检查组件存在性
     */
    @Override
    public void check() throws Exception {
        IStreamComponent.TypeEnum type = getComponentType();
        Set<String> components = ComponentDataSource.INSTANCE.getComponents(type);
        if (MXJudgeUtils.isEmpty(components)) {
            return;
        }

        IParser<String, T> parser = new ConfigurableComponentParser<>(type.getInstancePackagePrefix());
        T componentIt;
        Map<String, T> confComponents = new HashMap<>();

        for (String componentStrIt : components) {
            if (componentMap.containsKey(componentStrIt)// 既有组件表已经有实例了
                    || confComponents.containsKey(componentStrIt)) {// 本次配置组件也已经有实例了
                continue;
            }

            if (ComponentConfig.Format.isConfigurableComponent(componentStrIt)) {// 是配置组件
                componentIt = parser.parse(componentStrIt);// 生成实例
            } else {// 非配置组件不存在，报错
                throw new ClassNotFoundException(componentStrIt);
            }

            confComponents.put(componentStrIt, componentIt);
        }

        // 所有组件生成和检查通过，加入总表
        componentMap.putAll(confComponents);
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
