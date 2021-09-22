package mx.j2.recommend.filter.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.BloomUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Random;

/**
 * @author xiang.zhou
 * @description
 * @date 2020-07-17
 */
public class GuavaBigBloomFilter extends BaseFilter {

    private static Logger logger = LogManager.getLogger(GuavaBigBloomFilter.class);
    private static Random random = new Random();

    @Override
    public boolean prepare(BaseDataCollection baseDc) {
        String userId = BloomUtil.getUserId(baseDc);
        if (userId == null) {
            logger.error(" null uuid and userId for this request " + baseDc.req);
            return false;
        }
        // get big bloom filter
        baseDc.bigBloomFilter = MXDataSource.guavaBloom().getBigBloomFilter(userId);

        return true;
    }

    @Override
    public boolean isFilted(BaseDocument doc, BaseDataCollection dc) {
        if (doc == null || doc.id == null)
            return false;
        else {
            if (dc.bigBloomFilter != null && dc.bigBloomFilter.mightContain(doc.id)) {
                dc.bigBloomFilterCount++;
                //TODO: big bloom不完全过滤
                if (random.nextDouble() > 0.95) {
                    return false;
                } else {
                    return true;
                }
            }
        }
        return false;
    }
}
