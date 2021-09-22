package mx.j2.recommend.ruler.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.thrift.Result;
import mx.j2.recommend.util.MXJudgeUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.*;

/**
 * @author zhendong.wang
 * @version 1.0
 * @date 2021/5/11 下午5:50
 * @description 位置规则器基类
 */
public abstract class BasePositionRuler extends BaseRuler<BaseDataCollection>
        implements BaseDataCollection.IResult {
    static final Logger logger = LogManager.getLogger(BasePositionRuler.class);
    static final int INVALID_VALUE = -100;// 自定义的非法值

    @Override
    public boolean skip(BaseDataCollection baseDC) {
        return MXJudgeUtils.isEmpty(baseDC) || super.skip(baseDC);
    }

    @Override
    public void rule(BaseDataCollection dc) {
        List<Result> hostList = dc.getResultList();// 结果列表
        List<Result> hostListCopy = new ArrayList<>(hostList);// 结果列表副本，复原用
        List<Result> collectList = collect(dc, hostList);// 收集元素列表
        boolean ruled = false;// 默认处理失败

        // 两个集合都不能空
        if (MXJudgeUtils.isNotEmpty(hostList) && MXJudgeUtils.isNotEmpty(collectList)) {
            // 防止重写 collect 方法时元素没有从 hostList 中移除，这里 check 一下
            if (hostList.size() == hostListCopy.size()) {
                hostList.removeAll(collectList);
            }

            // 重新插入
            try {
                ruled = insert(hostList, collectList);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // 没有成功处理，复原
        if (!ruled) {
            hostList.clear();
            hostList.addAll(hostListCopy);

            logger.error("Will not perform ruler " + getName());
        }
    }

    @Override
    public void registerConfig(Map<String, Class> outConfMap) {
        super.registerConfig(outConfMap);
        outConfMap.put(KEY_RESULT, String.class);
    }

    @Override
    public String getResultKey() {
        return config.getString(KEY_RESULT);
    }

    /**
     * 收集要调序的元素
     * 必要时可重写
     */
    private List<Result> collect(BaseDataCollection dc, List<Result> hostList) {
        List<Result> collectList = new ArrayList<>();
        Iterator<Result> iterator = hostList.iterator();
        Result resultIt;

        while (iterator.hasNext() && collectList.size() < limit()) {
            resultIt = iterator.next();

            if (shouldCollect(dc, resultIt)) {
                collectList.add(resultIt);
                iterator.remove();
            }
        }

        return collectList;
    }

    /**
     * 子类调整规则入口
     *
     * @param hostList 原列表
     * @param subList  要调整位置的列表
     */
    abstract boolean insert(List<Result> hostList, List<Result> subList);

    /**
     * 最多收集的数量，默认 100 个，一般不会超过这个数
     * 子类可重写
     */
    int limit() {
        return 100;
    }

    /**
     * 是否被收集，默认收集指定拉链的数据
     * 子类可重写
     */
    boolean shouldCollect(BaseDataCollection dc, Result result) {
        return Objects.equals(result.getInternalUse().getRecallResultID(), getResultKey());
    }
}
