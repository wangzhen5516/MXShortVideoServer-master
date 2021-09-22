package mx.j2.recommend.filter.impl;

import com.newrelic.api.agent.Trace;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.manager.MXDataSource;

/**
 * @author xiang.zhou
 * @description
 * @date 2020-07-17
 */
@Deprecated
public class GuavaBloomFilter extends BaseFilter {

    @Override
    @Trace(dispatcher = true)
    public boolean prepare(BaseDataCollection baseDc) {
        if(baseDc.client.user.uuId.equals(baseDc.client.user.userId)) {
            return false;
        }
        baseDc.isHaveBloom = true;
        // only the user who has login
        MXDataSource.guavaBloom().getBloomFilter(baseDc.client.user.userId, baseDc);

        return true;
    }

    @Override
    public boolean isFilted(BaseDocument doc, BaseDataCollection dc) {
        if (doc == null || doc.id == null)
            return false;
        else {
            if (dc.guavaBloomFilter != null && dc.guavaBloomFilter.mightContain(doc.id)) {
                return true;
            }
        }
        return false;
    }
}
