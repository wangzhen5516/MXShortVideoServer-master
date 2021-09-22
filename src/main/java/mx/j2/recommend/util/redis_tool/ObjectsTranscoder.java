package mx.j2.recommend.util.redis_tool;

import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.LogTool;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;

/**
 * 
 * @author zhuowei
 */
public class ObjectsTranscoder<M extends Serializable> extends SerializeTranscoder {

	protected static final Logger logger = LogManager.getLogger(ObjectsTranscoder.class);

	@SuppressWarnings("unchecked")
	@Override
	public byte[] serialize(Object value) {
		if (value == null) {
			throw new NullPointerException("Can't serialize null");
		}
		byte[] result = null;
		ByteArrayOutputStream bos = null;
		ObjectOutputStream os = null;
		try {
			bos = new ByteArrayOutputStream();
			os = new ObjectOutputStream(bos);
			M m = (M) value;
			os.writeObject(m);
			os.close();
			bos.close();
			result = bos.toByteArray();
		} catch (IOException e) {
			throw new IllegalArgumentException("Non-serializable object", e);
		} finally {
			close(os);
			close(bos);
		}
		return result;
	}

	@SuppressWarnings("unchecked")
	@Override
	public M deserialize(byte[] in) {
		M result = null;
		ByteArrayInputStream bis = null;
		ObjectInputStream is = null;
		try {
			if (in != null) {
				bis = new ByteArrayInputStream(in);
				is = new ObjectInputStream(bis);
				result = (M) is.readObject();
				is.close();
				bis.close();
			}
		} catch (IOException e) {
			String message = String.format("Caught CNFE decoding %d bytes of data, Exception: %s", in.length) + e.fillInStackTrace();
			LogTool.printErrorLog(logger, DefineTool.ErrorNoEnum.NORMAL_ERROR.getErrorNo(), message, null, null);
		} catch (ClassNotFoundException e) {
			String message = String.format("Caught CNFE decoding %d bytes of data, Exception: %s", in.length, e);
			LogTool.printErrorLog(logger, DefineTool.ErrorNoEnum.NORMAL_ERROR.getErrorNo(), message, null, null);
		} finally {
			close(is);
			close(bis);
		}
		return result;
	}
}
