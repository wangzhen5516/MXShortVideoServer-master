package mx.j2.recommend.cache.impl;

import mx.j2.recommend.cache.ICache;
import mx.j2.recommend.component.stream.base.BaseStreamComponent;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.util.DefineTool;

/**
 * 缓存基类
 */
public abstract class BaseCache<T extends BaseDataCollection> extends BaseStreamComponent<T> implements ICache<T> {

    @Override
    public void doWork(T dc) {
        if (dc.getCacheOperation() == DefineTool.Cache.CacheOperationEnum.READ) {
            read(dc);
        } else if (dc.getCacheOperation() == DefineTool.Cache.CacheOperationEnum.WRITE) {
            write(dc);
        }
    }

    @Override
    public boolean skip(T dc) {
        if (dc.getCacheOperation() == DefineTool.Cache.CacheOperationEnum.READ) {
            return skipRead(dc);
        } else if (dc.getCacheOperation() == DefineTool.Cache.CacheOperationEnum.WRITE) {
            return skipWrite(dc);
        } else {
            return true;
        }
    }

    protected abstract boolean skipRead(T dc);

    protected abstract boolean skipWrite(T dc);
}
