package mx.j2.recommend.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Base64;

public class StringSerializationUtil {
    private static final Object EMPTY_OBJ = new Object();

    // serialize the object
    public static String serialize(Object myObject) {
        String serializedObject = "";
        try {
            ByteArrayOutputStream bo = new ByteArrayOutputStream();
            ObjectOutputStream so = new ObjectOutputStream(bo);
            so.writeObject(myObject);
            so.flush();
            serializedObject = Base64.getEncoder().encodeToString(bo.toByteArray());
        } catch (Exception e) {
            //System.out.println(e);
            e.printStackTrace();
        }
        return serializedObject;
    }

    // deserialize the object
    public static Object deserialize(String serializedObject) {
        try {
            byte b[] = Base64.getDecoder().decode(serializedObject);
            ByteArrayInputStream bi = new ByteArrayInputStream(b);
            ObjectInputStream si = new ObjectInputStream(bi);
            return si.readObject();
        } catch (Exception e) {
            //System.out.println(e);
            e.printStackTrace();
            return EMPTY_OBJ;
        }
    }
}
