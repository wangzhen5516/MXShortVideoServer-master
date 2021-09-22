package mx.j2.recommend.mixer.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.util.BaseMagicValueEnum;
import mx.j2.recommend.util.DefineTool;

/**
 * @author ：zhongrenli
 * @date ：Created in 4:46 下午 2021/2/2
 */
public class HighStrategyPoolMixer extends BaseStrategyPoolMixer {

    @Override
    public boolean skip(BaseDataCollection dc) {
        return super.skip(dc);
    }

    @Override
    public String getLevel() {
        return DefineTool.EsPoolLevel.HIGH.getLevel();
    }

    @Override
    public String getPoolLevel() {
        return BaseMagicValueEnum.STRATEGY_HIGH_LEVEL;
    }
}
