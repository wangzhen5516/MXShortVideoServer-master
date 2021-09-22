package mx.j2.recommend.task;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_model.interfaces.IDocumentProcessor;
import mx.j2.recommend.data_source.LocalCacheDataSource;
import mx.j2.recommend.hystrix.redis.ZrevRangeWithScoresStragegyCommand;
import mx.j2.recommend.manager.MXDataSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * @author ：zhongrenli
 * @date ：Created in 4:29 下午 2021/2/7
 */
public class StrategyPoolExecutor {

    private final ExecutorService workers;

    public StrategyPoolExecutor() {
        ThreadFactory namedThreadFactory = new ThreadFactoryBuilder().setNameFormat("strategy-pool-%s").build();
        workers = Executors.newFixedThreadPool(80, namedThreadFactory);
    }

    public void execute(String key,
                        Map<String, List<BaseDocument>> map,
                        int recallSize,
                        String recallName,
                        CountDownLatch cd) {
        CompletableFuture<Map<String, Double>> completableFuture = CompletableFuture.supplyAsync(() -> {
            ZrevRangeWithScoresStragegyCommand command = new ZrevRangeWithScoresStragegyCommand(key, recallSize);
            return command.execute();
        }, workers);

        completableFuture.whenComplete(((object, throwable) -> {
            try {
                List<String> ids = new ArrayList<>(object.keySet());
                List<BaseDocument> detailList = MXDataSource.details().get(ids, recallName);
                detailList.forEach(doc -> {
                    if (object.containsKey(doc.id)) {
                        doc.scoreDocument.setStrategyPoolScore(object.get(doc.id));
                    }
                });
                map.put(key, detailList);
                LocalCacheDataSource dataSource = MXDataSource.cache();
                dataSource.setTopHotTagDocumentCache(key, detailList);
            } finally {
                cd.countDown();
            }
        }));
    }

    /**
     * 从原方法拷贝而来，增加 3 个参数
     */
    public void execute(String key,
                        Map<String, List<BaseDocument>> map,
                        int recallSize,
                        int poolPriority,
                        String tag,
                        String table,
                        CountDownLatch cd) {
        CompletableFuture<Map<String, Double>> completableFuture = CompletableFuture.supplyAsync(() -> {
            ZrevRangeWithScoresStragegyCommand command = new ZrevRangeWithScoresStragegyCommand(key, recallSize);
            return command.execute();
        }, workers);

        completableFuture.whenComplete(((object, throwable) -> {
            try {
                List<String> ids = new ArrayList<>(object.keySet());

                // 为文档写入调试信息
                IDocumentProcessor processor = document -> {
                    document.setRecallTag(tag);
                    document.setRecallTable(table);
                    document.setStrategyPoolPriority(poolPriority);
                };

                List<BaseDocument> detailList = MXDataSource.details().get(ids, processor);
                detailList.forEach(doc -> {
                    if (object.containsKey(doc.id)) {
                        doc.scoreDocument.setStrategyPoolScore(object.get(doc.id));
                    }
                });
                map.put(key, detailList);
                LocalCacheDataSource dataSource = MXDataSource.cache();
                dataSource.setTopHotTagDocumentCache(key, detailList);
            } finally {
                cd.countDown();
            }
        }));
    }
}
