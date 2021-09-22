package mx.j2.recommend.data_source.database.cassandra.base;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.internal.core.metadata.DefaultEndPoint;
import mx.j2.recommend.data_source.database.base.BaseDatabase;

import java.net.InetSocketAddress;

/**
 * @author zhendong.wang
 * @version 1.0
 * @date 2021/7/7 下午7:17
 * @description CA 数据库基类
 */
public abstract class BaseCassandraDB extends BaseDatabase implements ICassandraDB {
    private CqlSession session;

    BaseCassandraDB(String keySpace) {
        session = CqlSession.builder()
                .addContactEndPoint(new DefaultEndPoint(new InetSocketAddress(getHost(), getPort())))
                .withLocalDatacenter(getDataCenter())
                .withKeyspace(keySpace)
                .build();
    }

    public abstract String getHost();

    public abstract int getPort();

    public abstract String getDataCenter();

    public CqlSession getSession() {
        return session;
    }
}
