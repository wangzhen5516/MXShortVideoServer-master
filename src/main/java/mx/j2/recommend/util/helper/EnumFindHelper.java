package mx.j2.recommend.util.helper;

import com.newrelic.api.agent.NewRelic;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.LogTool;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by zhongrenli on 2018/7/10.
 */
public class EnumFindHelper<T extends Enum<T>, K> {
    private static Logger log = LogManager.getLogger(EnumFindHelper.class);
    protected Map<K, T> map = new HashMap<K, T>();

    public EnumFindHelper(Class<T> clazz, EnumKeyGetter<T, K> keyGetter) {
        try {
            for (T enumValue : EnumSet.allOf(clazz)) {
                map.put(keyGetter.getKey(enumValue), enumValue);
            }
        } catch (Exception e) {
            log.error("EnumFindHelper error: " + e.fillInStackTrace());
            NewRelic.noticeError("EnumFindHelper error: " + e.fillInStackTrace());
            String message = String.format("EnumFindHelper error: %s",  e.fillInStackTrace());
            LogTool.printErrorLog(log, DefineTool.ErrorNoEnum.NORMAL_ERROR.getErrorNo(), message, null, null);
        }
    }

    public T find(K key, T defautValue) {
        T value = map.get(key);
        if (value == null) {
            value = defautValue;
        }
        return value;
    }

}
