package mx.j2.recommend.util.bean;

import com.google.common.hash.BloomFilter;

/**
 * @author ：zhongrenli
 * @date ：Created in 5:15 下午 2020/11/6
 */
public class BaseBloomFilter {
    BloomFilter<String> bloomFilter;

    long bloomSize;

    public static class UserIdHistoryBloomFilter extends BaseBloomFilter {}

    public static class UuidHistoryBloomFilter extends BaseBloomFilter {}

    public static class AdvertiseHistoryBloomFilter extends BaseBloomFilter {}

    String getName() {
        return this.getClass().getSimpleName();
    }

    public void clear() {
        this.bloomFilter = null;
        this.bloomSize = 0;
    }

    public BloomFilter<String> getBloomFilter() {
        return bloomFilter;
    }

    public void setBloomFilter(BloomFilter<String> bloomFilter) {
        this.bloomFilter = bloomFilter;
    }

    public long getBloomSize() {
        return bloomSize;
    }

    public void setBloomSize(long bloomSize) {
        this.bloomSize = bloomSize;
    }
}
