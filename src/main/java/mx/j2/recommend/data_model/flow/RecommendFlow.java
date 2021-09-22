package mx.j2.recommend.data_model.flow;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.ArrayList;
import java.util.List;

/**
 * 推荐流
 *
 * @author zhuowei
 */
@NotThreadSafe
public class RecommendFlow {
    public List<String> prejudgeList;

    public List<String> prepareList;

    public List<String> recallList;
    public List<String> preRecallList;

    public List<String> rulerList;

    public String ranker;

    public List<String> rerankList;

    public List<String> mixerList;

    public List<String> guaranteeMixerList;

    public List<String> scorerList;

    public List<String> adjusterList;

    public List<String> filterList;

    public List<String> preFilterList;

    public List<String> getFeatureList;

    public List<String> cacheList;

    public String rankModel;

    public String packer;

    public String fallback;

    public String name;

    public String interfaceName;

    public boolean historyCache;

    public boolean resultCache;

    public boolean userProfileCache;

    public int start;

    public int end;

    public String toString;

    public boolean interfaceResultCache;

    public List<String> cacheForRecallList;

    public MatchCondition matchCondition;

    public List<String> poolListInFlow;

    public String predictor;

    public List<String> realType;

    // 小流量命中区间，以支持不连续的区间配置
    public List<Range> rangeList;

    public String requestNum;

    /**
     * 构造函数
     */
    public RecommendFlow() {
        prepareList = new ArrayList<>();
        recallList = new ArrayList<>();
        rulerList = new ArrayList<>();
        scorerList = new ArrayList<>();
        adjusterList = new ArrayList<>();
        filterList = new ArrayList<>();
        getFeatureList = new ArrayList<>();
        cacheList = new ArrayList<>();
        rerankList = new ArrayList<>();
        packer = "";
        fallback = "";
        predictor = "";
        realType = new ArrayList<>();
        cacheForRecallList = new ArrayList<>();
        prejudgeList = new ArrayList<>();
        matchCondition = new MatchCondition();
        mixerList = new ArrayList<>();
        poolListInFlow = new ArrayList<>();
        guaranteeMixerList = new ArrayList<>();
        rangeList = new ArrayList<>();
        preFilterList = new ArrayList<>();
        requestNum = "";
        preRecallList = new ArrayList<>();
    }

    /**
     * 推荐流
     */
    public RecommendFlow(RecommendFlow other) {
        if (null != other.prepareList) {
            this.prepareList = new ArrayList<>(other.prepareList);
        }
        if (null != other.recallList) {
            this.recallList = new ArrayList<>(other.recallList);
        }
        if (null != other.preRecallList) {
            this.preRecallList = new ArrayList<>(other.preRecallList);
        }
        if (null != other.rangeList) {
            this.rangeList = new ArrayList<>(other.rangeList);
        }
        if (null != other.rulerList) {
            this.rulerList = new ArrayList<>(other.rulerList);
        }
        if (null != other.ranker) {
            this.ranker = other.ranker;
        }
        if (null != other.rerankList) {
            this.rerankList = new ArrayList<>(other.rerankList);
        }
        if (null != other.scorerList) {
            this.scorerList = new ArrayList<>(other.scorerList);
        }
        if (null != other.adjusterList) {
            this.adjusterList = new ArrayList<>(other.adjusterList);
        }
        if (null != other.filterList) {
            this.filterList = new ArrayList<>(other.filterList);
        }
        if (null != other.preFilterList) {
            this.preFilterList = new ArrayList<>(other.preFilterList);
        }
        if (null != other.requestNum) {
            this.requestNum = other.requestNum;
        }
        if (null != other.getFeatureList) {
            this.getFeatureList = new ArrayList<>(other.getFeatureList);
        }
        if (null != other.mixerList) {
            this.mixerList = new ArrayList<>(other.mixerList);
        }
        if (null != other.guaranteeMixerList) {
            this.guaranteeMixerList = new ArrayList<>(other.guaranteeMixerList);
        }
        if (null != other.cacheList) {
            this.cacheList = new ArrayList<>(other.cacheList);
        }
        if (null != other.poolListInFlow) {
            this.poolListInFlow = new ArrayList<>(other.poolListInFlow);
        }
        if (null != other.rankModel) {
            this.rankModel = other.rankModel;
        }
        if (null != other.packer) {
            this.packer = other.packer;
        }
        if (null != other.fallback) {
            this.fallback = other.fallback;
        }
        if (null != other.name) {
            this.name = other.name;
        }
        if (null != other.interfaceName) {
            this.interfaceName = other.interfaceName;
        }
        if (null != other.prejudgeList) {
            this.prejudgeList = new ArrayList<>(other.prejudgeList);
        }
        if (null != other.cacheForRecallList) {
            this.cacheForRecallList = new ArrayList<>();
            this.cacheForRecallList.addAll(other.cacheForRecallList);
        }
        if (null != other.matchCondition) {
            this.matchCondition = new MatchCondition(other.matchCondition);
        }
        if (null != other.predictor) {
            this.predictor = other.predictor;
        }
        if (null != other.realType) {
            this.realType = new ArrayList<>(other.realType);
        }
        this.start = other.start;
        this.end = other.end;
        this.toString = other.toString;
    }

    public RecommendFlow deepCopy() {
        return new RecommendFlow(this);
    }


    /**
     * toString()
     */
    @Override
    public String toString() {
        return toString;
    }
}
