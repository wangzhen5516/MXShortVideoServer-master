package mx.j2.recommend.cache.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.thrift.Result;
import mx.j2.recommend.util.DefineTool;

import javax.annotation.concurrent.ThreadSafe;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * 接口级别结果缓存基类
 */
@ThreadSafe
abstract class BaseInterfaceResultCache extends BaseResultCache {

    BaseInterfaceResultCache(Function<String, List<Result>> readFunction,
                             BiFunction<String, List<Result>, Void> writeFunction) {
        super(readFunction, writeFunction);
    }

    @Override
    String key(BaseDataCollection dc) {
        return DefineTool.toKey(getName(), dc.req.interfaceName);
    }
}
