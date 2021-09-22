package mx.j2.recommend.manager.impl;

import mx.j2.recommend.component.configurable.ConfigurableComponentParser;
import mx.j2.recommend.component.configurable.base.BaseComponentConfig;
import mx.j2.recommend.component.configurable.base.IConfigurable;
import mx.j2.recommend.component.configurable.base.IConfigurableComponent;
import mx.j2.recommend.component.configurable.ComponentConfig;
import mx.j2.recommend.component.stream.base.IStreamComponent;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.data_collection.InternalDataCollection;
import mx.j2.recommend.data_model.data_collection.OtherDataCollection;
import mx.j2.recommend.data_model.interfaces.IParser;
import mx.j2.recommend.data_source.ComponentDataSource;
import mx.j2.recommend.manager.IStreamComponentManager;
import mx.j2.recommend.manager.InternalManager;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.recall.InternalRecall;
import mx.j2.recommend.util.ClassUtil;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.LogTool;
import mx.j2.recommend.util.MXJudgeUtils;

import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

public enum InternalRecallManager implements InternalManager, IStreamComponentManager<InternalRecall> {
    INSTANCE;

    private HashMap<String, InternalRecall> recallMap;

    InternalRecallManager() {
        recallMap = new HashMap<>();
        try {
            List<Class> classes = ClassUtil.getAllClassByInterface(Class.forName("mx.j2.recommend.recall.InternalRecall"));
            for (Class c : classes) {
                if (!Modifier.isAbstract(c.getModifiers())) {
                    InternalRecall internalRecall = (InternalRecall) c.getConstructor().newInstance();

                    if (internalRecall instanceof IConfigurable) {
                        continue;
                    }

                    recallMap.put(internalRecall.getName(), internalRecall);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public String getName() {
        return this.getClass().getSimpleName();
    }

    @Override
    public void process(InternalDataCollection dc, OtherDataCollection otherDc) {
        for (String recallerName : dc.recommendFlow.recallList) {
            if (recallMap.containsKey(recallerName)) {
                recallMap.get(recallerName).procRecall(dc);
            }
        }

        otherDc.videoSearchRequestList.addAll(dc.videoSearchRequestList);
        MXDataSource.videoES().search(otherDc);

        otherDc.mergeForInternalInterface(dc);
    }

    @Override
    public void process(BaseDataCollection dc) {
        // do nothing
    }

    @Override
    public void preProcess(BaseDataCollection dc) {
        // do nothing
    }

    @Override
    public void inProcess(BaseDataCollection dc) {
        // do nothing
    }

    @Override
    public void postProcess(BaseDataCollection dc) {
        // do nothing
    }

    @Override
    public List<String> list(BaseDataCollection dc) {
        return null;
    }

    @Override
    public boolean skip(BaseDataCollection data) {
        return false;
    }

    @Override
    public String getComponentInterfacePath() {
        return "";
    }

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

    @Override
    public void check() throws Exception {
        initConfigurableRecall();
    }

    private void initConfigurableRecall() throws Exception {
        // 从数据源拿所有暂存的可配置召回器
        Set<String> recallList = ComponentDataSource.INSTANCE.getComponents(IStreamComponent.TypeEnum.RECALL);
        if (MXJudgeUtils.isEmpty(recallList)) {
            return;
        }

        IParser<String, IConfigurableComponent<BaseComponentConfig>> parser = new ConfigurableComponentParser("mx.j2.recommend.recall.impl.");
        IConfigurableComponent<BaseComponentConfig> recallIt = null;

        for (String recallStr : recallList) {
            if (recallMap.containsKey(recallStr)) {
                continue;
            }

            // 目前内部召回器认为是不可配置的，所以这块走不到，可以忽略先
            if (ComponentConfig.Format.isConfigurableComponent(recallStr)) {// 配置组件
                recallIt = parser.parse(recallStr);
            }

            if (recallIt instanceof InternalRecall) {
                recallMap.put(recallStr, (InternalRecall) recallIt);
            }
        }
    }

    @Override
    public InternalRecall getComponentInstance(String name) {
        return recallMap.get(name);
    }
}
