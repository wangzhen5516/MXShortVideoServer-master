package mx.j2.recommend.mixer.impl;

import mx.j2.recommend.util.BaseMagicValueEnum;
import mx.j2.recommend.util.DefineTool;

/**
 * @author ：zhongrenli
 * @date ：Created in 4:46 下午 2021/2/2
 */
public class LowStrategyPoolMixer extends BaseStrategyPoolMixer {
    @Override
    public String getLevel() {
        return DefineTool.EsPoolLevel.LOW.getLevel();
    }

    @Override
    public String getPoolLevel() {
        return BaseMagicValueEnum.STRATEGY_LOW_LEVEL;
    }
}
