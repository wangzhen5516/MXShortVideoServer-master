package mx.j2.recommend.task;

import com.google.common.hash.BloomFilter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import mx.j2.recommend.data_source.InactiveUserHistoryBloomDataSource;
import mx.j2.recommend.data_source.NewHistoryBloomCaDataSource;
import mx.j2.recommend.data_source.UserHistoryBloomDataSource;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.OptionalUtil;
import mx.j2.recommend.util.bean.BaseBloomFilter;
import mx.j2.recommend.util.bean.BloomInfo;

import java.util.concurrent.*;
import java.util.function.Consumer;

/**
 * @author ：zhongrenli
 * @date ：Created in 4:14 下午 2020/11/6
 */
public class TaskExecutor {
    private ExecutorService workers;

    public TaskExecutor(int threadNum) {
        ThreadFactory namedThreadFactory = new ThreadFactoryBuilder().setNameFormat("task-%s").build();
        this.workers = Executors.newFixedThreadPool(threadNum, namedThreadFactory);
    }

    public void apply(BaseBloomFilter baseBloomFilter,
                      Consumer<BloomInfo> callback,
                      String id,
                      String redisKey,
                      CountDownLatch cd,
                      boolean isVipUser) {
        CompletableFuture<BloomInfo> completableFuture = CompletableFuture.supplyAsync(() -> {
            UserHistoryBloomDataSource dataSource = MXDataSource.historyBloom();
            BloomFilter<String> bloomFilter = dataSource.getBloomFilter(id);

            if (OptionalUtil.ofNullable(bloomFilter).isPresent()) {
                baseBloomFilter.setBloomSize(dataSource.getBloomCapacity(id));
                baseBloomFilter.setBloomFilter(bloomFilter);
                return null;// 如果是redis有的话，返回null。不让execute回调callback
            }

            BloomInfo info = null;
            InactiveUserHistoryBloomDataSource inactiveUserHistoryBloomDataSource = MXDataSource.inactiveHistoryBloom();
            if (inactiveUserHistoryBloomDataSource.exists(redisKey, id)) {

                NewHistoryBloomCaDataSource newHistoryBloomCaDataSource = MXDataSource.historyBloomV2();
                info = newHistoryBloomCaDataSource.getBloomInfoFromCassandra(id, isVipUser);
                if (null != info) {
                    info.setBloomId(id);
                    baseBloomFilter.setBloomFilter(info.getBloomFilter());
                    baseBloomFilter.setBloomSize(info.getHistorySize());
                }
            }
            return info;
        }, workers);
        completableFuture.whenComplete(((bloomInfo, throwable) -> {
            try {
                callback.accept(bloomInfo);
            } finally {
                cd.countDown();
            }
        }));
    }
}
