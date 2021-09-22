package mx.j2.recommend.mixer.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;

import java.util.Random;

public class RealTimeStrategyPoolMixer1 extends RealTimeStrategyPoolMixer {
    private final Random rand = new Random();
    private final double KEEP_RATIO = 0.01;

    public RealTimeStrategyPoolMixer1() {
        RATIO = 0.067;
    }

    @Override
    public boolean skip(BaseDataCollection dc) {
        return rand.nextDouble() > KEEP_RATIO;
    }
}
