package mx.j2.recommend.util.bean;

import com.google.common.hash.BloomFilter;

/**
 * @author ：zhongrenli
 * @date ：Created in 11:27 上午 2020/11/6
 */
public class BloomInfo {
    private long historySize;

    private BloomFilter<String> bloomFilter;

    private String bloomString;

    private String bloomId;

    public long getHistorySize() {
        return historySize;
    }

    public void setHistorySize(long historySize) {
        this.historySize = historySize;
    }

    public BloomFilter<String> getBloomFilter() {
        return bloomFilter;
    }

    public void setBloomFilter(BloomFilter<String> bloomFilter) {
        this.bloomFilter = bloomFilter;
    }

    public String getBloomString() {
        return bloomString;
    }

    public void setBloomString(String bloomString) {
        this.bloomString = bloomString;
    }

    public String getBloomId() {
        return bloomId;
    }

    public void setBloomId(String bloomId) {
        this.bloomId = bloomId;
    }
}
