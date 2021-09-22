package mx.j2.recommend.fallback.impl;

import mx.j2.recommend.component.stream.base.BaseStreamComponent;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.fallback.IFallback;

/**
 * 数据保底召回基类
 */
public abstract class BaseFallback<T extends BaseDataCollection> extends BaseStreamComponent<T> implements IFallback<T> {

    @Override
    public void doWork(T dc) {
        fallback(dc);
    }
}
