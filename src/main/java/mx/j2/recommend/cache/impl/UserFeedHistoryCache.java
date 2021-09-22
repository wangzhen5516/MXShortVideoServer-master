package mx.j2.recommend.cache.impl;

import com.newrelic.api.agent.Trace;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;

import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class UserFeedHistoryCache extends BaseCache<BaseDataCollection> {

	@Override
	protected boolean skipRead(BaseDataCollection dc) {
		return false;
	}

	@Override
	@Trace(dispatcher = true)
	public void read(BaseDataCollection dc) {
	}

	@Override
	protected boolean skipWrite(BaseDataCollection dc) {
		return false;
	}

	@Override
	@Trace(dispatcher = true)
	public void write(BaseDataCollection dc) {

	}
}
