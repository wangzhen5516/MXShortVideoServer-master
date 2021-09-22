package mx.j2.recommend.ruler.impl;

import mx.j2.recommend.thrift.Result;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author zhendong.wang
 * @version 1.0
 * @date 2021/5/11 下午5:50
 * @description 步进式位置规则器
 * <p>
 * 例1：1，3，5，7，9...
 * 例2：1，4，7，10，13...
 */
@SuppressWarnings("unused")
public class PositionStepRuler extends BasePositionRuler {
    private static final String KEY_STEP = "step";// 步长
    private static final int CONFIG_VALUE_AUTO = -1;// 自动等比插入: XOOXOOX/XOOXOOXO
    private static final int STEP_MIN = 2;// 最小步长

    @Override
    public void registerConfig(Map<String, Class> outConfMap) {
        super.registerConfig(outConfMap);
        outConfMap.put(KEY_STEP, Integer.class);
    }

    @Override
    boolean insert(List<Result> hostList, List<Result> subList) {
        // 待插入数组只有一个元素，直接插到头部返回
        if (subList.size() == 1) {
            hostList.addAll(0, subList);
            return true;
        }

        int step = computeStep(hostList.size(), subList.size());

        // 非法值
        if (step == INVALID_VALUE) {
            logger.error("Invalid step config of ruler " + getName());
            return false;
        }

        // 按步长插入
        Iterator<Result> iterator = subList.iterator();
        int i = 0;
        Result resultIt;

        // 按步长间隔插入
        while (iterator.hasNext()) {
            resultIt = iterator.next();
            hostList.add(i, resultIt);
            i += step;
        }

        return true;
    }

    /**
     * 计算步长
     */
    private int computeStep(int hostSize, int subSize) {
        int step = getStepConfig();

        // 非法值
        if (step < STEP_MIN && step != CONFIG_VALUE_AUTO) {
            return INVALID_VALUE;
        }

        int retStep = step;// 实际计算出来的步长
        int needHostSizeAtLeast;// 至少需要的被插入数组的长度

        if (step == CONFIG_VALUE_AUTO) {// 等比步长
            if (subSize == 2) {// 两个元素，头部一个，中间一个:XOXO
                return (int) Math.ceil(hostSize / 2.0) + 1;
            } else {// 多个元素:XOOXOOX,XOOXOOXO,XOOOXOOO
                retStep = hostSize / (subSize - 1) + 1;
                return retStep < STEP_MIN ? INVALID_VALUE : retStep;
            }
        } else {// 固定步长
            needHostSizeAtLeast = (retStep - 1) * (subSize - 1);
            return hostSize < needHostSizeAtLeast ? INVALID_VALUE : retStep;
        }
    }

    /**
     * 拿到配置的步长
     */
    private int getStepConfig() {
        return config.getInt(KEY_STEP);
    }
}
