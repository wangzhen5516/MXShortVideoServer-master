package mx.j2.recommend.data_source.userprofile.base;

import mx.j2.recommend.data_source.base.BaseCassandraDS;

/**
 * @author zhendong.wang
 * @version 1.0
 * @date 2021/7/7 下午3:52
 * @description 个性化 CA 数据源基类
 */
public abstract class BaseUserProfileDS<R> extends BaseCassandraDS<String, R> {
    @Override
    protected String getKeySpace() {
        return Const.KeySpace.TAKATAK;
    }

    protected String buildQuery(String userId, String table) {
        return Const.Query.byUUID(userId, table);
    }

    public interface Const {
        interface KeySpace {
            String TAKATAK = "takatak";
        }

        interface Query {
            String ALL_BY_UUID = "select * from %s where uuid='%s';";

            static String byUUID(String uuid, String table) {
                return String.format(ALL_BY_UUID, table, uuid);
            }
        }
    }
}
