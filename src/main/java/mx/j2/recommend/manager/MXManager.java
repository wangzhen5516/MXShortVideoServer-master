package mx.j2.recommend.manager;

import mx.j2.recommend.manager.impl.*;

/**
 * managers 管理器
 */
public class MXManager {
    private final static MXManager INSTANCE = new MXManager();

    private CheckManager checkManager;
    private SkipManager skipManager;
    private MatchManager matchManager;
    private RecallManager recallManager;
    //private PreRecallManager preRecallManager;
    private RankerManager rankerManager;
    private FallbackManager fallbackManager;
    private RulerManager rulerManager;
    private FilterManager filterManager;
    private MixerManager mixerManager;
    private ScorerManager scorerManager;
    private PackerManager packerManager;
    private PredictManager predictManager;
    private PushCacheManager writeCacheManager;
    private PullCacheManager readCacheManager;
    private PushCacheForRecallManager writeCacheForRecallManager;
    private PullCacheForRecallManager readCacheForRecallManager;
    private PreFilterManager preFilterManager;
    private PrepareManager prepareManager;

    private MXManager() {
        checkManager = new CheckManager();
        skipManager = new SkipManager();
        matchManager = new MatchManager();
        recallManager = new RecallManager();
        rankerManager = new RankerManager();
        fallbackManager = new FallbackManager();
        rulerManager = new RulerManager();
        filterManager = new FilterManager();
        mixerManager = new MixerManager();
        scorerManager = new ScorerManager();
        packerManager = new PackerManager();
        predictManager = new PredictManager();
        writeCacheManager = new PushCacheManager();
        readCacheManager = new PullCacheManager();
        writeCacheForRecallManager = new PushCacheForRecallManager();
        readCacheForRecallManager = new PullCacheForRecallManager();
        preFilterManager = new PreFilterManager();
        prepareManager = new PrepareManager();
        //preRecallManager = new PreRecallManager();
    }

    public static CheckManager check() {
        return INSTANCE.checkManager;
    }

    public static SkipManager skip() {
        return INSTANCE.skipManager;
    }

    public static MatchManager match() {
        return INSTANCE.matchManager;
    }

    public static RecallManager recall() {
        return INSTANCE.recallManager;
    }

    public static RankerManager ranker() {
        return INSTANCE.rankerManager;
    }

    public static FallbackManager fallback() {
        return INSTANCE.fallbackManager;
    }

    public static RulerManager ruler() {
        return INSTANCE.rulerManager;
    }

    public static FilterManager filter() {
        return INSTANCE.filterManager;
    }

    public static MixerManager mixer() {
        return INSTANCE.mixerManager;
    }

    public static ScorerManager scorer() {
        return INSTANCE.scorerManager;
    }

    public static PackerManager packer() {
        return INSTANCE.packerManager;
    }

    public static PredictManager predictor() {
        return INSTANCE.predictManager;
    }

    public static PullCacheManager readCache() {
        return INSTANCE.readCacheManager;
    }

    public static PushCacheManager writeCache() {
        return INSTANCE.writeCacheManager;
    }

    public static PullCacheForRecallManager readRecallCache() {
        return INSTANCE.readCacheForRecallManager;
    }

    public static PushCacheForRecallManager writeRecallCache() {
        return INSTANCE.writeCacheForRecallManager;
    }

    public static PreFilterManager preFilter() {
        return INSTANCE.preFilterManager;
    }

    public static PrepareManager prepare() {
        return INSTANCE.prepareManager;
    }

//    public static PreRecallManager preRecall() {
//        return INSTANCE.preRecallManager;
//    }
}
