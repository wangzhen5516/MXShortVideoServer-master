package mx.j2.recommend.util;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;

/**
 * @author qiqi
 * @date 2020-07-20 17:16
 */
public class ThriftSerialUtil {
    private static final TSerializer serializer;
    private static final byte[] empty_byte_array;

    static {
        serializer = new TSerializer();
        empty_byte_array=new byte[]{};
    }
    public static byte[] serialize(TBase base) {
        try {
            if (null != base) {
                return serializer.serialize(base);
            } else {
                return empty_byte_array;
            }
        } catch (TException e) {
            e.printStackTrace();
        }
        return empty_byte_array;
    }
}
