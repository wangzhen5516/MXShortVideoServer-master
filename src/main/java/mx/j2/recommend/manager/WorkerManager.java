package mx.j2.recommend.manager;

import com.google.common.util.concurrent.AtomicDouble;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import mx.j2.recommend.util.MXJudgeUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author ：zhongrenli
 * @date ：Created in 2:44 下午 2020/12/8
 */
public class WorkerManager {

    private Map<String, Worker> workersMap;

    private final static int PERIOD_SECONDS = 5;

    public WorkerManager() {
        init();
    }

    private void init() {
        Worker hotTabInterfaceWorker = new Worker(1.0, "hot");
        Worker otherInterfaceWorker = new Worker(0.55, "other");
        //把real_time_action_version_1_0拆分出来

        Worker actionInterfaceWorker = new Worker(0.4, "action");
        actionInterfaceWorker.ErrorCount = new AtomicInteger(0);
        workersMap = new HashMap<>(16);
        workersMap.put("mx_hot_tab_internal_version_2_0", hotTabInterfaceWorker);
        workersMap.put("other", otherInterfaceWorker);
        workersMap.put("real_time_action_version_1_0", actionInterfaceWorker);

        ThreadFactory factory = new ThreadFactoryBuilder().setNameFormat("manager-worker-%s").build();
        ScheduledExecutorService scheduleService = Executors.newSingleThreadScheduledExecutor(factory);

        scheduleService.scheduleAtFixedRate(this::run, 5, PERIOD_SECONDS, TimeUnit.SECONDS);
    }

    public static class Worker {
        private Map<String, Long> threadTimestampMap;
        private AtomicInteger useCount;
        private volatile AtomicDouble useRate;
        private double limit;
        private String name;
        private volatile AtomicInteger ErrorCount;

        public Worker() {
            new Worker(0.25, "");
        }

        public Worker(double rate, String name) {
            threadTimestampMap = new ConcurrentHashMap<>();
            useCount = new AtomicInteger(0);
            useRate = new AtomicDouble(0);
            limit = rate;
            this.name = name;
        }

        public Map<String, Long> getThreadTimestampMap() {
            return threadTimestampMap;
        }

        public void setThreadTimestampMap(Map<String, Long> threadTimestampMap) {
            this.threadTimestampMap = threadTimestampMap;
        }

        public double getUseCountValue() {
            return useCount.get();
        }

        public AtomicInteger getUseCount() {
            return useCount;
        }

        public double getLimit() {
            return limit;
        }

        public AtomicInteger getErrorCount(){return ErrorCount;}

        public void addToMap(String k, long v) {
            threadTimestampMap.put(k, v);
        }

        public long removeFromMap(String k) {
            if (threadTimestampMap.containsKey(k)) {
                return threadTimestampMap.remove(k);
            }
            return -1;
        }

        public void setUseRate(double rate) {
            useRate.set(rate);
        }

        public double getUseRate() {
            return useRate.get();
        }

        public String getName() {
            return name;
        }

        @Override
        public String toString() {
            return "Worker{" +
                    "threadTimestampMap=" + threadTimestampMap +
                    ", useCount=" + useCount +
                    ", useRate=" + useRate +
                    ", limit=" + limit +
                    '}';
        }
    }

    public Worker getWorker(String interfaceName) {
        if (workersMap.containsKey(interfaceName)) {
            return workersMap.get(interfaceName);
        }
        return workersMap.get("other");
    }

    public boolean isIdle() {
        String main = "mx_hot_tab_internal_version_2_0";
        boolean isMainIdle = false;
        if (workersMap.containsKey(main)) {
            Worker mainWorker = workersMap.get(main);
            isMainIdle = mainWorker.getUseRate() < 0.7 * mainWorker.getLimit();
        }

        if (!isMainIdle) {
            return false;
        }

        String other = "other";
        boolean isOtherIdle = false;
        if (workersMap.containsKey(other)) {
            Worker otherWorker = workersMap.get(other);
            isOtherIdle = otherWorker.getUseRate() < 0.7;
        }
        return isOtherIdle;
    }

    private void run() {
        long timeThreshold = System.currentTimeMillis() - PERIOD_SECONDS * 1000;
        for (Map.Entry<String, Worker> entry : workersMap.entrySet()) {
            String interfaceNameGroup = entry.getKey();
            Worker worker = entry.getValue();
            if (MXJudgeUtils.isEmpty(worker.threadTimestampMap)) {
                continue;
            }

            for (Map.Entry<String, Long> entry1: worker.threadTimestampMap.entrySet()) {
                String threadName = entry1.getKey();
                if (entry1.getValue() < timeThreshold) {
                    System.out.println(String.format("thread-%s, timestamp: %s", threadName, entry1.getValue()));
                }
            }

            System.out.println(interfaceNameGroup + " " + worker.useCount + " " + worker.useRate);
        }
    }
}
