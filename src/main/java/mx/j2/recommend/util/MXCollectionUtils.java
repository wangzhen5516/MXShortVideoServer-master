package mx.j2.recommend.util;

import mx.j2.recommend.data_model.document.BaseDocument;
import org.apache.commons.collections.ListUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public abstract class MXCollectionUtils {

    public static final List<BaseDocument> EMPTY_LIST = new ArrayList<>();

    public static boolean isEmpty(Object[] array) {
        return array == null || array.length == 0;
    }

    public static boolean isNotEmpty(Object[] array) {
        return !isEmpty(array);
    }

    public static boolean isEmpty(Collection collection) {
        return collection == null || collection.isEmpty();
    }

    public static boolean isNotEmpty(Collection collection) {
        return !isEmpty(collection);
    }

    public static boolean isEmpty(Map map) {
        return map == null || map.isEmpty();
    }

    public static boolean isNotEmpty(Map map) {
        return !isEmpty(map);
    }

    /**
     * 求差集
     *
     * @param collection 原集
     * @param remove     删除集
     * @return 差集
     */
    public static List except(Collection collection, Collection remove) {
        return ListUtils.removeAll(collection, remove);
    }

    public static <T> List<T> shallowCopy(List<T> list) {
        return new ArrayList<>(list);
    }
}
