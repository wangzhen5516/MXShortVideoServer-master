package mx.j2.recommend.manager.impl;

import mx.j2.recommend.component.stream.base.IStreamComponent;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.prepare.impl.BasePrepare;

import java.util.List;

public class PrepareManager extends BaseConfStreamComponentManager<BasePrepare> {

    @Override
    public void preProcess(BaseDataCollection dc) {

    }

    @Override
    public void postProcess(BaseDataCollection dc) {

    }

    @Override
    public List<String> list(BaseDataCollection dc) {
        return dc.recommendFlow.prepareList;
    }

    @Override
    IStreamComponent.TypeEnum getComponentType() {
        return IStreamComponent.TypeEnum.PREPARE;
    }
}