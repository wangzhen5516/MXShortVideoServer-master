package mx.j2.recommend.component.base;

import mx.j2.recommend.cache.ICache;
import mx.j2.recommend.component.list.check.ICheck;
import mx.j2.recommend.component.list.match.IMatch;
import mx.j2.recommend.component.list.skip.ISkip;
import mx.j2.recommend.fallback.IFallback;
import mx.j2.recommend.filter.IFilter;
import mx.j2.recommend.mixer.IMixer;
import mx.j2.recommend.packer.IPacker;
import mx.j2.recommend.predictor.IPredictor;
import mx.j2.recommend.prefilter.IPreFilter;
import mx.j2.recommend.prepare.IPrepare;
import mx.j2.recommend.prerecall.IPreRecall;
import mx.j2.recommend.ranker.IRanker;
import mx.j2.recommend.recall.IRecall;
import mx.j2.recommend.ruler.IRuler;
import mx.j2.recommend.scorer.IScorer;
import mx.j2.recommend.util.DefineTool;

/**
 * 组件接口
 */
public interface IComponent {
    /**
     * 组件类型
     */
    enum TypeEnum {
        // 流组件
        CACHE("Cache", ICache.class),
        RECALL("Recall", IRecall.class),
        MIXER("Mixer", IMixer.class),
        FILTER("Filter", IFilter.class),
        FALLBACK("Fallback", IFallback.class),
        PACKER("Packer", IPacker.class),
        PREDICTOR("Predictor", IPredictor.class),
        RANKER("Ranker", IRanker.class),
        RULER("Ruler", IRuler.class),
        SCORER("Scorer", IScorer.class),
        PREFILTER("PreFilter", IPreFilter.class),
        PREPARE("Prepare", IPrepare.class),
        PRERECALL("PreRecall", IPreRecall.class),

        // 普通组件
        SKIP("Skip", ISkip.class),
        CHECK("Check", ICheck.class),
        MATCH("Match", IMatch.class);

        public final String title;
        public final Class ifClass;

        TypeEnum(String title, Class ifClass) {
            this.title = title;
            this.ifClass = ifClass;
        }

        /**
         * 组件包前缀
         */
        public String getInstancePackagePrefix() {
            return DefineTool.MAIN_PACKAGE_PREFIX + name().toLowerCase() + ".impl.";
        }

        /**
         * 流组件接口路径
         */
        public String getStreamComponentIfPath() {
            return getStreamComponentIfPackagePrefix() + ifClass.getSimpleName();
        }

        /**
         * 普通组件接口路径
         */
        public String getComponentIfPath() {
            return getComponentIfPackagePrefix() + ifClass.getSimpleName();
        }

        /**
         * 流组件接口路径前缀
         */
        private String getStreamComponentIfPackagePrefix() {
            return DefineTool.MAIN_PACKAGE_PREFIX + name().toLowerCase() + ".";
        }

        /**
         * 普通组件接口路径前缀
         */
        private String getComponentIfPackagePrefix() {
            return DefineTool.MAIN_PACKAGE_PREFIX + "component.list." + name().toLowerCase() + ".";
        }
    }

    /**
     * 返回组件的名字，是组件就应该有名字
     */
    String getName();
}
