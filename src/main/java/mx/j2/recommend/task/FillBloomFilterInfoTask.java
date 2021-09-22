package mx.j2.recommend.task;

import mx.j2.recommend.data_source.UserHistoryBloomDataSource;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.OptionalUtil;
import mx.j2.recommend.util.bean.BaseBloomFilter;
import mx.j2.recommend.util.bean.BloomInfo;

import java.util.concurrent.CountDownLatch;

/**
 * @author ：zhongrenli
 * @date ：Created in 4:06 下午 2020/11/6
 */
public class FillBloomFilterInfoTask extends BaseTask{

    public FillBloomFilterInfoTask(TaskExecutor executor) {
        super(executor);
    }

    public void execute(BaseBloomFilter baseBloomFilter, CountDownLatch cd, String id, String redisKey, boolean isVipUser) {
        this.executor.apply(baseBloomFilter, this::callback, id, redisKey, cd, isVipUser);
    }

    @Override
    void callback(BloomInfo bloomInfo) {
        UserHistoryBloomDataSource dataSource = MXDataSource.historyBloom();

        OptionalUtil.ofNullable(bloomInfo)
                .getUtil(BloomInfo::getBloomFilter)
                .ifPresent(bloomFilter1 -> {
                    dataSource.setBloomCapacity(bloomInfo.getBloomId(), bloomInfo.getHistorySize());
                    dataSource.setBloomFilter(bloomInfo.getBloomId(), bloomFilter1);
        });
    }
}
