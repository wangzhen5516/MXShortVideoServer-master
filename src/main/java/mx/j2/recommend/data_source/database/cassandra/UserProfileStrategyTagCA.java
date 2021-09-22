package mx.j2.recommend.data_source.database.cassandra;

import mx.j2.recommend.data_source.database.cassandra.base.BaseUserProfileTagCA;
import mx.j2.recommend.hystrix.cassandra.StrategyCassandraQueryStringResultCommand;

/**
 * @author zhendong.wang
 * @version 1.0
 * @date 2021/7/7 下午7:22
 * @description 策略标签 CA
 */
public class UserProfileStrategyTagCA extends BaseUserProfileTagCA {

    UserProfileStrategyTagCA(String keySpace) {
        super(keySpace);
    }

    @Override
    public String query(String query, String column) {
        return new StrategyCassandraQueryStringResultCommand(
                getSession(),
                query,
                column).execute();
    }
}
