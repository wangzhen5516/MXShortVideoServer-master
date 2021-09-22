package mx.j2.recommend.server;

import com.google.common.util.concurrent.RateLimiter;
import com.newrelic.api.agent.NewRelic;
import com.newrelic.api.agent.Trace;
import mx.j2.recommend.conf.Conf;
import mx.j2.recommend.manager.DataSourceManager;
import mx.j2.recommend.manager.IComponentManager;
import mx.j2.recommend.manager.MXManager;
import mx.j2.recommend.manager.WorkerManager;
import mx.j2.recommend.manager.impl.InternalRecallManager;
import mx.j2.recommend.stream.RecommendStream;
import mx.j2.recommend.stream.impl.*;
import mx.j2.recommend.task.DoRecommendExecutor;
import mx.j2.recommend.thrift.*;
import mx.j2.recommend.util.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import javax.annotation.concurrent.ThreadSafe;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 推荐入口
 *
 * @author zhuowei
 */
@ThreadSafe
public class RecommendHandler implements RecommendService.Iface {
    private static Logger logger = LogManager.getLogger(RecommendHandler.class);
    private final static Response EMPTY_RESPONSE = new Response();
    private final static InternalResponse EMPTY_INTERNAL_RESPONSE = new InternalResponse();

    private FeedStream feedStream;
    private OtherStream otherStream;
    private BannerStream bannerStream;
    private FetchTabsStream fetchTabsStream;
    private NullStream nullStream;
    private InternalStream internalStream;

    private RateLimiter rateLimiter;
    private RateLimiter rateLimiterForInternal;

    private WorkerManager workerManager;

    private DoRecommendExecutor executor;

    private final static long defaultTimeDelay = 0L;
    private final static long questTimeOut = Conf.getRequestTimeOut();

    private final Map<Integer, Double> grayScaleMap = new LinkedHashMap<Integer, Double>() {
        {
            put(5, 100D);
            put(10, 300D);
            put(12, 500D);
            put(15, 800D);
            put(18, Conf.getTotalRateForAllInterfaces());
        }
    };

    // 所有依赖数据源的组件管理器
    private List<IComponentManager> dependOnDataSourceManagers;

    /**
     * 构造函数
     */
    public RecommendHandler() {
        init(0.1, 0.1);
    }

    public void init(double totalRateForAllInterfaces, double totalRateForInternalInterfaces) {
        rateLimiter = RateLimiter.create(totalRateForAllInterfaces);
        rateLimiterForInternal = RateLimiter.create(totalRateForInternalInterfaces);
        feedStream = new FeedStream();
        otherStream = new OtherStream();
        bannerStream = new BannerStream();
        fetchTabsStream = new FetchTabsStream();
        internalStream = new InternalStream();
        nullStream = new NullStream();
        DataSourceManager.INSTANCE.init();
        onDataSourcePrepared();

        workerManager = new WorkerManager();
        executor = new DoRecommendExecutor();
        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        executorService.schedule(this::changeRateLimiterForRecommend, 30, TimeUnit.SECONDS);
        executorService.schedule(this::changeRateLimiterForInternal, 5, TimeUnit.MINUTES);
    }

    /**
     * 初始化所有依赖数据源的组件管理器列表
     */
    private void initDependOnDataSourceManagers() {
        dependOnDataSourceManagers = new ArrayList<>();
        //dependOnDataSourceManagers.add(MXManager.preRecall());
        dependOnDataSourceManagers.add(MXManager.prepare());
        dependOnDataSourceManagers.add(MXManager.recall());
        dependOnDataSourceManagers.add(MXManager.mixer());
        dependOnDataSourceManagers.add(MXManager.filter());
        dependOnDataSourceManagers.add(MXManager.ruler());
        dependOnDataSourceManagers.add(InternalRecallManager.INSTANCE);
    }

    /**
     * 通知数据源准备完毕，想干啥干啥现在
     */
    private void onDataSourcePrepared() {
        initDependOnDataSourceManagers();

        for (IComponentManager manager : dependOnDataSourceManagers) {
            manager.onDataSourcePrepared();
        }
    }

    /**
     * 测试rpc接口
     */
    @Trace(dispatcher = true)
    @Override
    public String test(String word) throws TException {
        NewRelic.setTransactionName("test", "test");
        return "TEST OK";
    }

    @Trace(dispatcher = true)
    @Override
    public Response recommend(Request req) {
        String interfaceName = "recommend";

        // 保底数据功能要用到 interfaceName，所以提前到这里转换
        new InterfaceNameTransformateTool().replaceInterfaceName(req);
        NewRelic.setTransactionName(interfaceName, req.getInterfaceName());

        // 拿到具体的推荐流
        RecommendStream stream = requestProxy(req);

        if (!rateLimiter.tryAcquire()) {
            // 实时action接口的话不重试
            if ("real_time_action_version_1_0".equals(req.getInterfaceName())) {
                logger.error("real_time_action_version_1_0 is empty,rateLimiter is full");
                return EMPTY_RESPONSE;
            }

//            if (!req.isRetryRequest) {
//                logger.error(String.format("request is retry, as traffic is so heavy, req:[%s]", req.toString()));
//                retryLog(req);
//                return stream.setRetryFlag(req);
//            }
            retryLog(req);
            return stream.setRetryFlag(req);
        }

        boolean check = checkParamInReq(req, interfaceName);
        if (!check) {
            return EMPTY_RESPONSE;
        }

        Response response = EMPTY_RESPONSE;

        // 如果不是强制保底，先走推荐
        WorkerManager.Worker worker = null;
        if (!stream.hasFallback(req.interfaceName, true)) {
            worker = workerManager.getWorker(req.interfaceName);
            if (Double.compare(worker.getUseRate(), worker.getLimit()) > 0 && !workerManager.isIdle()) {
                String msg = String.format(
                        "interface %s, now useRate: %s, go beyond limit: %s",
                        req.interfaceName,
                        worker.getUseRate(),
                        worker.getLimit());
                System.out.println(msg);
                //如果不是action接口，NewRelic直接报error；如果是action接口并且到达设定的error的统计数量，NewRelic报error
                if (!"real_time_action_version_1_0".equals(req.getInterfaceName()) || isCounterReachLimit(worker.getErrorCount(), 1000)) {
                    NewRelic.noticeError(String.format("interface %s, go beyond limit: %s", worker.getName(), worker.getLimit()));
                }
                return EMPTY_RESPONSE;
            } else {
                add(worker);
            }
            CountDownLatch cd = new CountDownLatch(1);
            AtomicReference<Response> reference = new AtomicReference<>(new Response());
            executor.execute(stream, req, reference, cd);

            try {
                cd.await(1500, TimeUnit.MILLISECONDS);
                response = reference.get();
            } catch (Exception e) {
                response = stream.setRetryFlag(req);
                NewRelic.noticeError(String.format("interface %s, timeout", req.interfaceName));
            }
        }

        // 如果推荐过后空数据，再走保底
        if (isEmptyResponse(response) && stream.hasFallback(req.interfaceName, false)) {
            response = stream.fallback(req);
        }

        if (response == null) {
            response = EMPTY_RESPONSE;
        }
        reduce(worker);

        if (response.needRetry) {
            retryLog(req);
        }

        LogTool.printResponse(logger, response, req.logId);
        return response;
    }

    @Override
    public Response fetchBanner(Request req) throws TException {
        if (!rateLimiter.tryAcquire()) {
            logger.error(String.format("request is thrown, as traffic is so heavy, req:[%s]", req.toString()));
            return EMPTY_RESPONSE;
        }
        String interfaceName = "fetchBanner";
        boolean check = checkParamInReq(req, interfaceName);
        if (!check) {
            return EMPTY_RESPONSE;
        }
        NewRelic.setTransactionName(interfaceName, req.getInterfaceName());
        Response response;
        RecommendStream stream = requestProxy(req);
        CountDownLatch cd = new CountDownLatch(1);
        AtomicReference<Response> reference = new AtomicReference<>(new Response());
        executor.execute(stream, req, reference, cd);
        try {
            cd.await(1500, TimeUnit.MILLISECONDS);
            response = reference.get();
        } catch (Exception e) {
            response = stream.setRetryFlag(req);
            NewRelic.noticeError(String.format("interface %s, timeout", req.interfaceName));
        }

        if (response == null) {
            response = EMPTY_RESPONSE;
        }
        if (response.needRetry) {
            retryLog(req);
        }
        return response;
    }

    @Override
    public Response fetchTabs(Request req) throws TException {
        if (!rateLimiter.tryAcquire()) {
            logger.error(String.format("request is thrown, as traffic is so heavy, req:[%s]", req.toString()));
            return EMPTY_RESPONSE;
        }
        NewRelic.setTransactionName("fetchTabs", req.getInterfaceName());
        Response response;
        RecommendStream stream = requestProxy(req);
        CountDownLatch cd = new CountDownLatch(1);
        AtomicReference<Response> reference = new AtomicReference<>(new Response());
        executor.execute(stream, req, reference, cd);
        try {
            cd.await(1500, TimeUnit.MILLISECONDS);
            response = reference.get();
        } catch (Exception e) {
            response = stream.setRetryFlag(req);
            NewRelic.noticeError(String.format("interface %s, timeout", req.interfaceName));
        }
        if (response == null) {
            response = EMPTY_RESPONSE;
        }
        if (response.needRetry) {
            retryLog(req);
        }
        return response;
    }

    @Trace(dispatcher = true)
    @Override
    public InternalResponse internalRecommend(InternalRequest internalReq) throws TException {
        String interfaceName = "internalRecommend";

        if (MXStringUtils.isEmpty(internalReq.getInterfaceName())) {
            return EMPTY_INTERNAL_RESPONSE;
        }
        NewRelic.setTransactionName(interfaceName, internalReq.getInterfaceName());

        if (!rateLimiterForInternal.tryAcquire()) {
            logger.error(String.format("request is thrown, as traffic is so heavy, req:[%s]", internalReq.toString()));
            return EMPTY_INTERNAL_RESPONSE;
        }

        InternalResponse response;
        RecommendStream stream = internalRequestProxy(internalReq);
        response = stream.internalRecommend(internalReq);

        if (response == null) {
            response = EMPTY_INTERNAL_RESPONSE;
        }
        return response;
    }

    private void changeRateLimiterForInternal() {
        OptionalUtil.ofNullable(rateLimiterForInternal)
                .ifPresent(rateLimiter -> {
                    rateLimiter.setRate(5);
                });
    }

    private void changeRateLimiterForRecommend() {
        OptionalUtil.ofNullable(rateLimiter)
                .ifPresent(rateLimiter -> grayScaleMap.forEach((k, v) -> {
                    try {
                        TimeUnit.SECONDS.sleep(k);
                        rateLimiter.setRate(v);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }));
    }

    @Override
    public Response fetchStatus(Request req) throws TException {
        if (!rateLimiter.tryAcquire()) {
            logger.error(String.format("request is thrown, as traffic is so heavy, req:[%s]", req.toString()));
            return EMPTY_RESPONSE;
        }
        String interfaceName = "fetchStatus";
        boolean check = checkParamInReq(req, interfaceName);
        if (!check) {
            return EMPTY_RESPONSE;
        }
        NewRelic.setTransactionName(interfaceName, req.getInterfaceName());
        Response response;
        RecommendStream stream = requestProxy(req);
        CountDownLatch cd = new CountDownLatch(1);
        AtomicReference<Response> reference = new AtomicReference<>(new Response());
        executor.execute(stream, req, reference, cd);
        try {
            cd.await(1500, TimeUnit.MILLISECONDS);
            response = reference.get();
        } catch (Exception e) {
            response = stream.setRetryFlag(req);
            NewRelic.noticeError(String.format("interface %s, timeout", req.interfaceName));
        }
        if (response == null) {
            response = EMPTY_RESPONSE;
        }
        if (response.needRetry) {
            retryLog(req);
        }
        return response;
    }

    private boolean checkParamInReq(Request req, String interfaceName) {
        if (MXJudgeUtils.isEmpty(req.platformId)) {
            logger.error("platformId is required! request is " + req);
            return false;
        }

        long timeDelay = defaultTimeDelay;
        long requestTime = System.currentTimeMillis();
        long execTime = System.currentTimeMillis();
        if (MXJudgeUtils.isNotEmpty(req.getTimeSign())) {
            try {
                requestTime = Long.parseLong(req.getTimeSign());
                execTime = System.currentTimeMillis();
                timeDelay = execTime - requestTime;
            } catch (Exception e) {
                NewRelic.noticeError("parse timeSign failed: " + req.toString());
                String message = String.format("parse timeSign failed: %s", req.toString());
                LogTool.printErrorLog(logger, DefineTool.ErrorNoEnum.NORMAL_ERROR.getErrorNo(), message, req, null);
            }
        }
        req.setExecTimeSign(String.valueOf(execTime));
        req.setExecTimeDelay(String.valueOf(timeDelay));

        if (timeDelay >= questTimeOut) {
            NewRelic.setTransactionName(interfaceName, "timeout request");
            NewRelic.noticeError(String.format("request timeout req[%s] reqTime[%s] execTime[%s] timeDelay[%s]", req.toString(), requestTime, execTime, timeDelay));
            LogTool.printErrorLog(logger, DefineTool.ErrorNoEnum.NORMAL_ERROR.getErrorNo(), String.format("request timeout req[%s] reqTime[%s] execTime[%s] timeDelay[%s]", req.toString(), requestTime, execTime, timeDelay), req, null);
            return false;
        }

        return true;
    }

    private void add(WorkerManager.Worker worker) {
        if (null == worker) {
            System.out.println("worker is null!");
            return;
        }
        worker.addToMap(Thread.currentThread().getName(), System.currentTimeMillis());
        AtomicInteger count = worker.getUseCount();
        int num = count.intValue();

        while (!count.compareAndSet(num, num + 1)) {
            num = count.intValue();
        }
        worker.setUseRate(num / (Conf.getWorkThreadNum() * 1.0));
    }

    private void reduce(WorkerManager.Worker worker) {
        if (null == worker) {
            System.out.println("worker is null!");
            return;
        }
        worker.removeFromMap(Thread.currentThread().getName());
        AtomicInteger count = worker.getUseCount();
        int num = count.intValue();

        while (!count.compareAndSet(num, num - 1)) {
            num = count.intValue();
        }
        worker.setUseRate(num / (Conf.getWorkThreadNum() * 1.0));
    }

    private void retryLog(Request req) {
        LogTool.printErrorLog(logger, DefineTool.ErrorNoEnum.RETRY_REQUEST.getErrorNo(), String.format("interface %s, retry", req.interfaceName), req, null);
    }

    private RecommendStream requestProxy(Request request) {
        String stream = DefineTool.FlowInterface.findFlowInterfaceByName(request.getInterfaceName(), DefineTool.FlowInterface.DEFAULT).getStream();
        if (stream == null) {
            NewRelic.noticeError(String.format("Unknown interfacename: %s, please checkout request! -> ", request.toString()));
            String message = String.format("Unknown interfacename, please checkout request! ->%s", request.toString());
            LogTool.printErrorLog(logger, "1", message, request, null);
            return this.nullStream;
        }

        switch (stream) {
            case "feed":
                return this.feedStream;
            case "other":
                return this.otherStream;
            case "banner":
                return this.bannerStream;
            case "fetch_tabs":
                return this.fetchTabsStream;
            default:
                return this.nullStream;
        }
    }

    private RecommendStream internalRequestProxy(InternalRequest request) {
        String stream = DefineTool.FlowInterface.findFlowInterfaceByName(request.getInterfaceName(), DefineTool.FlowInterface.DEFAULT).getStream();
        if (stream == null) {
            NewRelic.noticeError(String.format("Unknown interfacename: %s, please checkout request! -> ", request.toString()));
            String message = String.format("Unknown interfacename, please checkout request! ->%s", request.toString());
            LogTool.printErrorLogForInternal(logger, "1", message, request, null);
            return this.nullStream;
        }

        if ("internal".equals(stream)) {
            return this.internalStream;
        } else {
            return this.nullStream;
        }
    }

    private boolean isEmptyResponse(Response response) {
        return response == null || MXJudgeUtils.isEmpty(response.resultList);
    }

    /**
     * 判断目前累积的error数量是不是到达上报的要求
     *
     * @param count 目前统计的error数量
     * @param limit 上报一次NewRelic需要累积的error数量
     * @return
     */
    private boolean isCounterReachLimit(AtomicInteger count, int limit) {
        int num = (count.intValue() + 1) % limit;
        return num == 0;
    }
}
