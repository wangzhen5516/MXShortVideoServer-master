package mx.j2.recommend.data_model.interfaces;

/**
 * @author zhendong.wang
 * @version 1.0
 * @date 2021/7/23 上午11:55
 * @description
 */
public interface IField<T> {
    default boolean exists(Class<T> clazz, String field) {
        try {
            clazz.getDeclaredField(field);
            return true;
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        }
        return false;
    }
}
