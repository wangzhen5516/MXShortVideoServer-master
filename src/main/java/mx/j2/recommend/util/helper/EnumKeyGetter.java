package mx.j2.recommend.util.helper;

/**
 * Created by zhongrenli on 2018/7/10.
 */
public interface EnumKeyGetter<T extends Enum<T>, K> {
    K getKey(T enumValue);
}
