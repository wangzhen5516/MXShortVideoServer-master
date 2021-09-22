package mx.j2.recommend.cache;

public interface ICache<T> {

    /**
     * 读缓存
     */
    void read(T data);

    /**
     * 写缓存
     */
    void write(T data);
}
