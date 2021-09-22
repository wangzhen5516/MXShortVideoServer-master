package mx.j2.recommend.task;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import mx.j2.recommend.stream.RecommendStream;
import mx.j2.recommend.thrift.Request;
import mx.j2.recommend.thrift.Response;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author ：zhongrenli
 * @date ：Created in 4:29 下午 2021/3/9
 */
public class DoRecommendExecutor {

    private final ExecutorService workers;

    public DoRecommendExecutor() {
        ThreadFactory namedThreadFactory = new ThreadFactoryBuilder().setNameFormat("do-recommend-pool-%s").build();
        workers = Executors.newFixedThreadPool(100, namedThreadFactory);
    }

    public void execute(RecommendStream stream,
                            Request request,
                            AtomicReference<Response> response,
                            CountDownLatch cd) {
        CompletableFuture<Response> completableFuture =
                CompletableFuture.supplyAsync(() -> {
                    Response r;
                    try {
                        return stream.recommend(request);
                    } catch (Exception e) {
                        r = stream.setRetryFlag(request);
                    }
                    return r;
                }, workers)
                        .exceptionally(ex -> new Response().setNeedRetry(true));
        completableFuture.whenComplete(((object, throwable) -> {
            try {
                response.set(object.deepCopy());
            } finally {
                cd.countDown();
            }
        }));
    }
}
