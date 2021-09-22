package mx.j2.recommend.data_source.userprofile.tag;

import mx.j2.recommend.data_source.database.cassandra.CassandraDBFactory;
import mx.j2.recommend.data_source.database.cassandra.base.ICassandraDB;
import mx.j2.recommend.data_source.userprofile.tag.base.BaseUserProfileTagDS;

/**
 * @author zhendong.wang
 * @version 1.0
 * @date 2021/7/7 下午4:44
 * @description 个性化策略标签 CA 数据源
 */
public class UserProfileStrategyTagDS extends BaseUserProfileTagDS {

    @Override
    protected ICassandraDB newCassandra() {
        return CassandraDBFactory.newUserProfileStrategyTagCA(getKeySpace());
    }
}
