package mx.j2.recommend.filter.impl;

import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import mx.j2.recommend.conf.Conf;
import mx.j2.recommend.data_source.GuavaBloomDataSource;

import java.nio.charset.StandardCharsets;

/**
 * @author xiang.zhou
 * @description
 * @date 2020-07-19
 */
public class CommandTest {
    public static void main(String[] args) {
        Conf.loadConf("./conf/conf.sample.properties");
        StatefulRedisClusterConnection<byte[], byte[]> connection = new GuavaBloomDataSource().getGuavaBloomRedisConn();
        byte[] username = "11".getBytes(StandardCharsets.UTF_8);
        byte[] ret = connection.sync().get(username);
        System.out.println(ret);
    }
}
