package mx.j2.recommend.data_source.database.cassandra.base;

/**
 * @author zhendong.wang
 * @version 1.0
 * @date 2021/7/7 下午7:21
 * @description CA 数据库接口
 */
public interface ICassandraDB {
    /**
     * 查询单一列
     */
    String query(String query, String column);
}
