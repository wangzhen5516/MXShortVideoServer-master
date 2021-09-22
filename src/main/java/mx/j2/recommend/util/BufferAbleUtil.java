package mx.j2.recommend.util;

import com.datastax.oss.protocol.internal.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;

/**
 * @author ：zhongrenli
 * @date ：Created in 8:56 下午 2020/10/27
 */
public interface BufferAbleUtil extends Serializable {

    static final Logger LOGGER = LoggerFactory.getLogger(BufferAbleUtil.class);

    /**
     * xx
     * @return
     */
    default ByteBuffer serialize() {
        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(bytes);) {
            oos.writeObject(this);
            String hexString = Bytes.toHexString(bytes.toByteArray());
            return Bytes.fromHexString(hexString);
        } catch (IOException e) {
            LOGGER.error("Serializing buffer able object error", e);
            return null;
        }
    }

    /**
     * xx
     * @param bytes
     * @return
     */
    public static BufferAbleUtil deserialize(ByteBuffer bytes) {
        String hx = Bytes.toHexString(bytes);
        ByteBuffer ex = Bytes.fromHexString(hx);
        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(ex.array()));) {
            return (BufferAbleUtil) ois.readObject();
        } catch (ClassNotFoundException | IOException e) {
            LOGGER.error("Deserializing buffer able object error", e);
            return null;
        }
    }
}
