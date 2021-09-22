package mx.j2.recommend.cache.impl;

import com.newrelic.api.agent.Trace;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.thrift.Result;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXJudgeUtils;

import javax.annotation.concurrent.ThreadSafe;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * 结果缓存基类
 */
@ThreadSafe
public abstract class BaseResultCache extends BaseCache<BaseDataCollection> {

    /**
     * 拉取内容方法
     */
    private Function<String, List<Result>> readFunction;

    /**
     * 写入内容方法
     */
    private BiFunction<String, List<Result>, Void> writeFunction;

    BaseResultCache(Function<String, List<Result>> readFunction,
                    BiFunction<String, List<Result>, Void> writeFunction) {
        this.readFunction = readFunction;
        this.writeFunction = writeFunction;
    }

    @Override
    protected boolean skipRead(BaseDataCollection dc) {
        return false;
    }

    @Override
    @Trace(dispatcher = true)
    public void read(BaseDataCollection dc) {
        List<Result> resultList = readFunction.apply(key(dc));

        if (MXJudgeUtils.isNotEmpty(resultList)) {
            dc.data.result.resultList.addAll(resultList);
            dc.resultFromMap.put(getName(), DefineTool.RecallFrom.LOCAL.getName());
            dc.util.cacheStatus = DefineTool.Cache.CacheStatus.IgnoreAll;
        }
    }

    @Override
    protected boolean skipWrite(BaseDataCollection dc) {
        return DefineTool.Cache.CacheStatus.IgnoreAll == dc.util.cacheStatus;
    }

    @Override
    @Trace(dispatcher = true)
    public void write(BaseDataCollection dc) {
        writeFunction.apply(key(dc), dc.data.result.resultList);
    }

    /**
     * 缓存键
     */
    abstract String key(BaseDataCollection dc);
}
