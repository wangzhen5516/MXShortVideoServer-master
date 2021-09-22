package mx.j2.recommend.filter.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import java.util.Optional;

/**
 * @author liangjie.feng
 * @version 1.0
 * @date 2021/5/12 下午7:49
 * @description
 */
public class UserRecommendBloomNoPrepareFilter extends BaseFilter {

    @Override
    public boolean isFilted(BaseDocument doc, BaseDataCollection dc) {

        if (Optional.ofNullable(dc.bloomFilterCollections.getUserIdHistoryBloomFilter()).isPresent()) {
            if (dc.bloomFilterCollections.getUserIdHistoryBloomFilter().mightContain(doc.id)) {
                dc.recallFilterCount.add("userIdHistoryBloom");
                return true;
            }
        }
        if (Optional.ofNullable(dc.bloomFilterCollections.getUuidHistoryBloomFilter()).isPresent()) {
            if (dc.bloomFilterCollections.getUuidHistoryBloomFilter().mightContain(doc.id)) {
                dc.recallFilterCount.add("uuIdHistoryBloom");
                return true;
            }
        }
        if (Optional.ofNullable(dc.bloomFilterCollections.getAdvertiseIdBloomFilter()).isPresent()) {
            if (dc.bloomFilterCollections.getAdvertiseIdBloomFilter().mightContain(doc.id)) {
                dc.recallFilterCount.add("adIdHistoryBloom");
                return true;
            }
        }

        return false;
    }
}
