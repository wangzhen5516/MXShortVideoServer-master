package mx.j2.recommend.recall.impl;

/**
 * @author ：zhongrenli
 * @date ：Created in 2:39 下午 2021/3/22
 */
public class VideosOfThePublisherFromCassandraRecall extends BaseVideosOfThePublisherFromCassandraRecall {

    @Override
    boolean getPrivateField() {
        return false;
    }
}
