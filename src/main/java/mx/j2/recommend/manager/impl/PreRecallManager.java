package mx.j2.recommend.manager.impl;

import mx.j2.recommend.component.base.IComponent;
import mx.j2.recommend.component.stream.base.IStreamComponent;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.recall.impl.BaseRecall;

import java.util.List;

/**
 * @author qiqi
 * @date 2021-07-01 14:47
 */
public class PreRecallManager extends BaseConfStreamComponentManager<BaseRecall> {


    @Override
    public void preProcess(BaseDataCollection dc) throws Exception {

    }

    @Override
    public void postProcess(BaseDataCollection dc) throws Exception {

    }


    @Override
    IComponent.TypeEnum getComponentType() {
        return IStreamComponent.TypeEnum.PRERECALL;
    }

    @Override
    public List<String> list(BaseDataCollection dc) {
        return dc.recommendFlow.preRecallList;
    }
}
