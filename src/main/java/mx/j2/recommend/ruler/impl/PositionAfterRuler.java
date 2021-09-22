package mx.j2.recommend.ruler.impl;

import mx.j2.recommend.thrift.Result;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * @author zhendong.wang
 * @version 1.0
 * @date 2021/5/11 下午5:50
 * @description "在第 N 个位置后"规则器
 */
@SuppressWarnings("unused")
public class PositionAfterRuler extends BasePositionRuler {
    private static final String KEY_AFTER = "after";
    private static final String KEY_SHUFFLE = "shuffle";// 是否打散放入

    private Random random = new Random();

    @Override
    public void registerConfig(Map<String, Class> outConfMap) {
        super.registerConfig(outConfMap);
        outConfMap.put(KEY_AFTER, Integer.class);
        outConfMap.put(KEY_SHUFFLE, Boolean.class);
    }

    /**
     * 将目标元素放到第 N 个元素的后面
     *
     * @param hostList 原列表
     * @param subList  要调整位置的元素
     */
    @Override
    boolean insert(List<Result> hostList, List<Result> subList) {
        int position = computePosition(hostList);

        if (position == INVALID_VALUE) {
            logger.error("Invalid position config of ruler " + getName());
            return false;
        }

        if (getShuffleConfig()) {// 需要打散
            if (subList.size() == 1) {// 只有一个元素，随机选一个位置插入
                int bound = hostList.size() - position + 1;
                position += random.nextInt(bound);
                hostList.addAll(position, subList);
            } else {// 元素集合，需要将集合打散插入
                shuffleAfter(hostList, position, subList);
            }
        } else {// 非打散，直接插入到目标位置
            hostList.addAll(position, subList);
        }

        return true;
    }

    /**
     * 将目标集合打散放入到目标位置
     */
    private void shuffleAfter(List<Result> hostList, int position, List<Result> subList) {
        if (position == hostList.size()) {// 该位置后没有数据了，直接追加到尾部
            hostList.addAll(subList);
        } else {// 该位置后有数据，随机插入
            Iterator<Result> iterator = subList.iterator();
            Result resultIt;

            while (iterator.hasNext()) {
                resultIt = iterator.next();
                int bound = hostList.size() - position + 1;// 加 1 是允许追加到最后
                int randomPos = position + random.nextInt(bound);
                hostList.add(randomPos, resultIt);
            }
        }
    }

    /**
     * 计算插入位置
     *
     * @param hostList 原列表
     */
    private int computePosition(List<Result> hostList) {
        int position = getAfterConfig();

        // 非法值
        if (position <= 0) {
            return INVALID_VALUE;
        }

        // 如果配置值溢出，视同置底
        if (position > hostList.size()) {
            position = hostList.size();
        }

        return position;
    }

    /**
     * 拿到目标位置
     */
    private int getAfterConfig() {
        return config.getInt(KEY_AFTER);
    }

    /**
     * 是否打散插入
     */
    private boolean getShuffleConfig() {
        return config.getBoolean(KEY_SHUFFLE);
    }
}
