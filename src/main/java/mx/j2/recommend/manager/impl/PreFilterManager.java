package mx.j2.recommend.manager.impl;

import mx.j2.recommend.component.base.IComponent;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.prefilter.impl.BasePreFilter;
import mx.j2.recommend.util.MXJudgeUtils;

import java.util.List;
import java.util.Map;

/**
 * @author liangjie.feng
 * @version 1.0
 * @date 2021/5/11 下午7:55
 * @description
 */
public class PreFilterManager extends BaseConfStreamComponentManager<BasePreFilter> {
    @Override
    public void preProcess(BaseDataCollection dc) throws Exception {

    }

    @Override
    public void postProcess(BaseDataCollection dc) throws Exception {

    }

    @Override
    public List<String> list(BaseDataCollection dc) {
        return dc.recommendFlow.preFilterList;
    }

    @Override
    IComponent.TypeEnum getComponentType() {
        return IComponent.TypeEnum.PREFILTER;
    }

    public void inProcess(Map<String, List<String>> poolToIdListMap, BaseDataCollection dc, String recallName) {
        List<String> list = list(dc);
        if (MXJudgeUtils.isEmpty(list)) {
            return;
        }

        BasePreFilter componentIt;
        for (String componentNameIt : list) {
            componentIt = getComponentInstance(componentNameIt);
            if (componentIt != null) {
                componentIt.doWork(poolToIdListMap, dc, recallName);
            }
        }
    }
}
