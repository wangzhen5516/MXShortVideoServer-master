package mx.j2.recommend.util.bean;

import com.google.common.hash.BloomFilter;

/**
 * @author ：zhongrenli
 * @date ：Created in 11:54 上午 2020/11/6
 */
public class BloomFilterCollections {
    private BloomFilter<String> userIdHistoryBloomFilter;
    private long userIdHistoryBloomSize;

    private BloomFilter<String> uuidHistoryBloomFilter;
    private long uuidHistoryBloomSize;

    private BloomFilter<String> advertiseIdBloomFilter;
    private long advertiseIdBloomSize;

    public BloomFilter<String> getUserIdHistoryBloomFilter() {
        return userIdHistoryBloomFilter;
    }

    public void setUserIdHistoryBloomFilter(BloomFilter<String> userIdHistoryBloomFilter) {
        this.userIdHistoryBloomFilter = userIdHistoryBloomFilter;
    }

    public long getUserIdHistoryBloomSize() {
        return userIdHistoryBloomSize;
    }

    public void setUserIdHistoryBloomSize(long userIdHistoryBloomSize) {
        this.userIdHistoryBloomSize = userIdHistoryBloomSize;
    }

    public BloomFilter<String> getUuidHistoryBloomFilter() {
        return uuidHistoryBloomFilter;
    }

    public void setUuidHistoryBloomFilter(BloomFilter<String> uuidHistoryBloomFilter) {
        this.uuidHistoryBloomFilter = uuidHistoryBloomFilter;
    }

    public long getUuidHistoryBloomSize() {
        return uuidHistoryBloomSize;
    }

    public void setUuidHistoryBloomSize(long uuidHistoryBloomSize) {
        this.uuidHistoryBloomSize = uuidHistoryBloomSize;
    }

    public BloomFilter<String> getAdvertiseIdBloomFilter() {
        return advertiseIdBloomFilter;
    }

    public void setAdvertiseIdBloomFilter(BloomFilter<String> advertiseIdBloomFilter) {
        this.advertiseIdBloomFilter = advertiseIdBloomFilter;
    }

    public long getAdvertiseIdBloomSize() {
        return advertiseIdBloomSize;
    }

    public void setAdvertiseIdBloomSize(long advertiseIdBloomSize) {
        this.advertiseIdBloomSize = advertiseIdBloomSize;
    }

    public void clear() {
        this.userIdHistoryBloomFilter = null;
        this.uuidHistoryBloomFilter = null;
        this.advertiseIdBloomFilter = null;

        this.userIdHistoryBloomSize = 0;
        this.uuidHistoryBloomSize = 0;
        this.advertiseIdBloomSize = 0;
    }

    @Override
    public String toString() {
        return "BloomFilterCollections{" +
                "userIdHistoryBloomFilter=" + userIdHistoryBloomFilter +
                ", userIdHistoryBloomSize=" + userIdHistoryBloomSize +
                ", uuidHistoryBloomFilter=" + uuidHistoryBloomFilter +
                ", uuidHistoryBloomSize=" + uuidHistoryBloomSize +
                ", advertiseIdBloomFilter=" + advertiseIdBloomFilter +
                ", advertiseIdBloomSize=" + advertiseIdBloomSize +
                '}';
    }
}
