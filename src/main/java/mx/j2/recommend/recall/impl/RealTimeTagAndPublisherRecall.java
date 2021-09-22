package mx.j2.recommend.recall.impl;


/**
 * @author qiqi
 * @date 2021-03-09 20:29
 */
public class RealTimeTagAndPublisherRecall extends RealTimePublisherHeatRecall {

    @Override
    public boolean useHashTag() {
        return true;
    }
}
