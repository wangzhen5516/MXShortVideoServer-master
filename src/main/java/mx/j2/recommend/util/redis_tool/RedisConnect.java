package mx.j2.recommend.util.redis_tool;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Created by yue.wang on 17/2/8.
 * 非cluster模式下redis连接
 */
public class RedisConnect {

    private String hostName;
    private int port;
    private Jedis jedis = null;
    private JedisPool jedisPool = null;
    private static final int MAXIDLE = 200; //pool内最大有几个idle的redis实例
    private static final int MAXTOTAL = 200; //pool内最大有几个redis实例
    private static final long WAITTIME = 1000L; //获取连接时候最大等待毫秒数
    private static final int MINIDLE = 100;  //pool内最少偶几个idle的redis实例


    public void setJedis(Jedis jedis) {
        this.jedis = jedis;
    }

    public Jedis getJedis() {
        return jedis;
    }

    public RedisConnect(String hostName, int port) {
        this.hostName = hostName;
        this.port = port;
    }

    public void simpleConnect(){
        //简单连接实例
        try {
            this.jedis = new Jedis(this.hostName, this.port);
        }
        catch(Exception e){
            //写log
            e.printStackTrace();
        }
    }

    /**
     * 构建redis 非cluster模式的连接池
     */
    public void getPool(){
        //连接池方式例
        if(this.jedisPool == null){
            JedisPoolConfig configRedis = new JedisPoolConfig();
            configRedis.setMaxTotal(MAXTOTAL);
            configRedis.setMaxIdle(MAXIDLE);
            configRedis.setMinIdle(MINIDLE);
            configRedis.setMaxWaitMillis(WAITTIME);
            configRedis.setTestOnBorrow(true);
            this.jedisPool = new JedisPool(configRedis, this.hostName, this.port);
        }
    }

    public JedisPool getJedisPool() {
        return jedisPool;
    }


}
