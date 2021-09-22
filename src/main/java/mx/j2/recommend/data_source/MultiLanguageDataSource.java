package mx.j2.recommend.data_source;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_model.flow.LanguageNameToIdFlowParser;
import mx.j2.recommend.data_model.flow.UserSelectLanguageFlow;
import mx.j2.recommend.data_model.flow.UserSelectLanguageFlowParser;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.task.LanguageTask;
import mx.j2.recommend.util.MXJudgeUtils;

import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * @author ：zhongrenli
 * @date ：Created in 3:31 下午 2020/10/15
 */
public class MultiLanguageDataSource extends BaseDataSource{

    /**
     * 保存用户选择语言与结果掺入百分比
     */
    private Map<String, UserSelectLanguageFlow> userSelectLanguageFlowMap;

    /**
     * 语言 name 到 id 的映射
     */
    private Map<String, String> languageNameToIdMap;

    /**
     * 保存各个语言的结果
     */
    private Map<String, List<BaseDocument>> languageResultMap;

    /**
     * 语言数据前缀
     */
    private static final String REDIS_KEY_PREFIX = "tophot_language_%s";

    /**
     * 保存 Future 句柄
     */
    private Map<String, Future<List<BaseDocument>>> saveFutureMap;

    public MultiLanguageDataSource() {
        init();
    }

    private void init() {
        userSelectLanguageFlowMap = UserSelectLanguageFlowParser.parse();
        assert null != userSelectLanguageFlowMap;

        languageNameToIdMap = LanguageNameToIdFlowParser.parse();
        assert null != languageNameToIdMap;

        languageResultMap = new ConcurrentHashMap<>(languageNameToIdMap.size() * 2);
        saveFutureMap = new ConcurrentHashMap<>(languageNameToIdMap.size() * 2);

        ThreadFactory factory = new ThreadFactoryBuilder().setNameFormat("multi-language-%s").build();
        ScheduledExecutorService scheduleService = Executors.newScheduledThreadPool(
                languageNameToIdMap.size(),
                factory);

        scheduleService.scheduleAtFixedRate(this::run, 3, 600, TimeUnit.SECONDS);

    }

    private void runWithFuture() {
        ThreadFactory factory = new ThreadFactoryBuilder().setNameFormat("multi-language-%s").build();
        ExecutorService exs = new ThreadPoolExecutor(
                languageNameToIdMap.size(),
                languageNameToIdMap.size(),
                1000L,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(),
                factory,
                new ThreadPoolExecutor.AbortPolicy());

        languageNameToIdMap.forEach((k, v) -> {
            LanguageTask task = new LanguageTask(v, this.getName());
            Future<List<BaseDocument>> future = exs.submit(task);
            saveFutureMap.put(v, future);
        });

        saveFutureMap.forEach((k, v) -> {
            int times = 3;
            while(times > 0) {
                if (v.isDone() && !v.isCancelled()) {
                    try {
                        languageResultMap.put(k, v.get());
                        break;
                    } catch (InterruptedException | ExecutionException e) {
                        e.printStackTrace();
                    }
                }
                times--;
                try {
                    TimeUnit.MICROSECONDS.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    private void run() {
        ElasticCacheSource elasticCacheSource = MXDataSource.redis();
        languageNameToIdMap.forEach((k,v) -> {
            Map<String, Double> languageCalculateMap = elasticCacheSource.getManualControltCache(
                    String.format(REDIS_KEY_PREFIX, v),
                    1000);

            if (MXJudgeUtils.isEmpty(languageCalculateMap)) {
                return;
            }

            List<BaseDocument> documents = MXDataSource.details().get(languageCalculateMap.keySet(), this.getName());
            if (MXJudgeUtils.isEmpty(documents)) {
                return;
            }

            languageResultMap.put(v, documents);
            
            try {
                TimeUnit.SECONDS.sleep(2);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
    }

    public List<BaseDocument> getDocumentListByLanguage(String language) {
        return languageResultMap.getOrDefault(language, null);
    }

    public UserSelectLanguageFlow getUserSelectLanguageFlowByLanguageName(String name) {
        return userSelectLanguageFlowMap.getOrDefault(name, null);
    }
}
