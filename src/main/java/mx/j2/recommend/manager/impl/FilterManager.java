package mx.j2.recommend.manager.impl;

import mx.j2.recommend.component.stream.base.IStreamComponent;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.filter.impl.BaseFilter;

import java.util.List;

public class FilterManager extends BaseConfStreamComponentManager<BaseFilter> {

    @Override
    IStreamComponent.TypeEnum getComponentType() {
        return IStreamComponent.TypeEnum.FILTER;
    }

    @Override
    public List<String> list(BaseDataCollection dc) {
        return dc.recommendFlow.filterList;
    }

    @Override
    public void preProcess(BaseDataCollection dc) {

    }

    @Override
    public void postProcess(BaseDataCollection dc) {

    }
}