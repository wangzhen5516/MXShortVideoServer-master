package mx.j2.recommend.manager.impl;

import mx.j2.recommend.cache.impl.BaseCache;
import mx.j2.recommend.component.stream.base.IStreamComponent;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.util.DefineTool;

public abstract class BaseCacheManager extends BaseStreamComponentManager<BaseCache> {

    @Override
    IStreamComponent.TypeEnum getComponentType() {
        return IStreamComponent.TypeEnum.CACHE;
    }

    @Override
    public void preProcess(BaseDataCollection dc) {
        dc.setCacheOperation(getOperation());
    }

    @Override
    public void postProcess(BaseDataCollection dc) {

    }

    /**
     * 当前操作
     */
    abstract DefineTool.Cache.CacheOperationEnum getOperation();
}