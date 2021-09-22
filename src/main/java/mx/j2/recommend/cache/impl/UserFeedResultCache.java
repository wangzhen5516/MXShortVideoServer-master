package mx.j2.recommend.cache.impl;

import com.newrelic.api.agent.Trace;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_source.ElasticCacheSource;
import mx.j2.recommend.manager.MXDataSource;

import javax.annotation.concurrent.ThreadSafe;

import static mx.j2.recommend.util.DefineTool.Cache.CacheStatus.IgnoreAll;

@ThreadSafe
public class UserFeedResultCache extends BaseCache<BaseDataCollection> {

	@Override
	protected boolean skipRead(BaseDataCollection dc) {
		return false;
	}

	@Override
	@Trace(dispatcher = true)
	public void read(BaseDataCollection dc) {
		ElasticCacheSource elasticCacheSource = MXDataSource.redis();
		//elasticCacheSource.getResultCache(dc);
		elasticCacheSource.getResultCacheByLettuce(dc);
	}

	@Override
	protected boolean skipWrite(BaseDataCollection dc) {
		return IgnoreAll == dc.util.cacheStatus;
	}

	@Override
	@Trace(dispatcher = true)
	public void write(BaseDataCollection dc) {
		ElasticCacheSource elasticCacheSource = MXDataSource.redis();
		//elasticCacheSource.setResultCache(dc);
		elasticCacheSource.setResultCacheByLettuce(dc);
	}
}
