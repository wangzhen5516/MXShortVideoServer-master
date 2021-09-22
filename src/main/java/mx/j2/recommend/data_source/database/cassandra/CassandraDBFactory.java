package mx.j2.recommend.data_source.database.cassandra;

import mx.j2.recommend.data_source.database.cassandra.base.ICassandraDB;

/**
 * @author zhendong.wang
 * @version 1.0
 * @date 2021/7/7 下午7:39
 * @description CA 数据库工厂
 */
public class CassandraDBFactory {
    public static ICassandraDB newUserProfileStrategyTagCA(String keyspace) {
        return new UserProfileStrategyTagCA(keyspace);
    }
}
