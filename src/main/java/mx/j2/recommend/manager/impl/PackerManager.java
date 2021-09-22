package mx.j2.recommend.manager.impl;

import mx.j2.recommend.component.stream.base.IStreamComponent;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.packer.impl.BasePacker;

import java.util.Collections;
import java.util.List;

public class PackerManager extends BaseStreamComponentManager<BasePacker> {

    @Override
    public void preProcess(BaseDataCollection dc) {

    }

    @Override
    public void postProcess(BaseDataCollection dc) {

    }

    @Override
    public List<String> list(BaseDataCollection dc) {
        return Collections.singletonList(dc.recommendFlow.packer);
    }

    @Override
    IStreamComponent.TypeEnum getComponentType() {
        return IStreamComponent.TypeEnum.PACKER;
    }
}
