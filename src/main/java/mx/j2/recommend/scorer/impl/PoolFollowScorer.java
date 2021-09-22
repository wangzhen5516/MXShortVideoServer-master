package mx.j2.recommend.scorer.impl;

import com.google.common.base.Charsets;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.newrelic.api.agent.Trace;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_source.LocalCacheDataSource;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.bean.BloomInfo;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class PoolFollowScorer extends BaseScorer<BaseDataCollection> {

    public static final int SCORE_NUM = 15;

    @Override
    public boolean skip(BaseDataCollection dc) {
        return false;
    }

    /**
     * simplescorer
     */
    @Override
    @Trace(dispatcher = true)
    public void score(BaseDataCollection dc) {
        String userId = dc.client.user.userId;
        LocalCacheDataSource localCacheDataSource = MXDataSource.cache();
        byte[] bloom = localCacheDataSource.getBloomFilter(userId);
        BloomFilter<String> bloomFilter = null;
        try {
            // read from local
            if (bloom != null) {
                bloomFilter = BloomFilter.readFrom(new ByteArrayInputStream(bloom), (Funnel<String>) (s, primitiveSink) -> primitiveSink.putString(s, Charsets.UTF_8));
            }

            if (bloomFilter == null) {
                //TODO 性能瓶颈
                BloomInfo bloomInfo = MXDataSource.userFollowBloom().getBloomInfoFromCassandra(userId);
                if (bloomInfo != null) {
                    bloomFilter = bloomInfo.getBloomFilter();
                }
            }
            if (bloomFilter == null) {
                return;
            }
            // write to local
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            bloomFilter.writeTo(out);
            byte[] bloomBytes = out.toByteArray();

            localCacheDataSource.setBloomFilter(userId, bloomBytes);
        } catch (IOException e) {
            e.printStackTrace();
        }
        // score and rank the poolMap
        BloomFilter<String> finalBloomFilter = bloomFilter;

        dc.poolToDocumentListMap.entrySet().stream().filter(e -> {
            String key = e.getKey();
            key = key.replace("taka_flowpool_lv", "");
            try {
                String[] key_item = key.split("_");
                int lv = Integer.parseInt(key_item[0]);
                if (lv <= 6) {
                    return true;
                }
            } catch (Exception ex) {
                return false;
            }
            return false;
        }).forEach(
                e -> {
                    List<BaseDocument> documents = e.getValue();
                    int i = 0;
                    Iterator<BaseDocument> itr = documents.iterator();
                    List<BaseDocument> toHead = new ArrayList<>();
                    while (itr.hasNext() && i < SCORE_NUM) {
                        BaseDocument d = itr.next();
                        if (finalBloomFilter.mightContain(d.publisher_id)) {
                            d.isFollowed = true;
                            toHead.add(d);
                            itr.remove();
                            i++;
                        }
                    }
                    documents.addAll(0, toHead);
                }
        );
    }
}
