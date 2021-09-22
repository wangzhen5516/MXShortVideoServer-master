package mx.j2.recommend.data_source.database.cassandra.base;

import mx.j2.recommend.conf.Conf;

/**
 * @author zhendong.wang
 * @version 1.0
 * @date 2021/7/7 下午7:22
 * @description 个性化 CA 基类
 */
public abstract class BaseUserProfileCA extends BaseCassandraDB {

    public BaseUserProfileCA(String keySpace) {
        super(keySpace);
    }

    @Override
    public String getHost() {
        return Conf.getStrategyTagCassandraHost();
    }

    @Override
    public int getPort() {
        return Conf.getStrategyTagCassandraPort();
    }

    @Override
    public String getDataCenter() {
        return Conf.getStrategyTagCassandraDc();
    }
}
