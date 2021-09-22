package mx.j2.recommend.manager.impl;

import mx.j2.recommend.component.stream.base.IStreamComponent;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.recall.impl.BaseRecall;

import java.util.List;

import static mx.j2.recommend.util.DefineTool.Cache.CacheStatus.IgnoreRecall;

/**
 * recall管理器，管理所有的recall
 *
 * @author zhuowei
 */
public class RecallManager extends BaseConfStreamComponentManager<BaseRecall> {

    @Override
    public boolean skip(BaseDataCollection dc) {
        return super.skip(dc) || IgnoreRecall == dc.util.cacheStatus;
    }

    @Override
    public void preProcess(BaseDataCollection dc) {

    }

    @Override
    public void postProcess(BaseDataCollection dc) throws Exception {
        MXDataSource.videoES().search(dc);
        MXDataSource.strategyES().search(dc);
        MXDataSource.videoESV7().newSearch(dc);

        dc.recordRecallInfo();
        dc.merge();
    }

    @Override
    public List<String> list(BaseDataCollection dc) {
        return dc.recommendFlow.recallList;
    }

    @Override
    IStreamComponent.TypeEnum getComponentType() {
        return IStreamComponent.TypeEnum.RECALL;
    }
}