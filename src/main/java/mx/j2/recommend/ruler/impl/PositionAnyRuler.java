package mx.j2.recommend.ruler.impl;

import mx.j2.recommend.thrift.Result;

import java.util.List;
import java.util.Map;

/**
 * @author zhendong.wang
 * @version 1.0
 * @date 2021/5/11 下午5:50
 * @description 任意位置规则器
 */
@SuppressWarnings("unused")
public class PositionAnyRuler extends BasePositionRuler {
    private static final String KEY_AT = "at";
    private static final int CONFIG_VALUE_BOTTOM = -1;// 置底配置的值

    @Override
    public void registerConfig(Map<String, Class> outConfMap) {
        super.registerConfig(outConfMap);
        outConfMap.put(KEY_AT, Integer.class);
    }

    /**
     * 将子列表插入原列表的指定位置
     *
     * @param hostList 原列表
     * @param subList  要调整位置的元素
     */
    @Override
    boolean insert(List<Result> hostList, List<Result> subList) {
        int position = computePosition(hostList);
        hostList.addAll(position, subList);
        return true;
    }

    /**
     * 计算插入位置
     *
     * @param hostList 原列表
     */
    private int computePosition(List<Result> hostList) {
        int position = getAtConfig();

        // 配置值溢出，视同置底
        if (position < 1 || position > hostList.size()) {
            position = CONFIG_VALUE_BOTTOM;
        }

        return position == CONFIG_VALUE_BOTTOM ? hostList.size() : position - 1;
    }

    /**
     * 拿到配置的目标位置
     */
    private int getAtConfig() {
        return config.getInt(KEY_AT);
    }
}
