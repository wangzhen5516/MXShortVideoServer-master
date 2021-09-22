package mx.j2.recommend.cache.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.thrift.Result;
import mx.j2.recommend.util.DefineTool;

import javax.annotation.concurrent.ThreadSafe;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * 资源 ID 级别结果缓存基类
 */
@ThreadSafe
abstract class BaseResourceIdResultCache extends BaseInterfaceResultCache {

    BaseResourceIdResultCache(Function<String, List<Result>> readFunction,
                              BiFunction<String, List<Result>, Void> writeFunction) {
        super(readFunction, writeFunction);
    }

    @Override
    String key(BaseDataCollection dc) {
        return DefineTool.toKey(super.key(dc), dc.req.resourceId);
    }
}
