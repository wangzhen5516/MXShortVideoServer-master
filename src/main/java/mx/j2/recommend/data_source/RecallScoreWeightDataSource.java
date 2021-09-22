//package mx.j2.recommend.data_source;
//
//import mx.j2.recommend.component.stream.base.IStreamComponent;
//import mx.j2.recommend.manager.impl.RecallManager;
//import mx.j2.recommend.recall.IRecall;
//import mx.j2.recommend.util.MXJudgeUtils;
//import org.apache.logging.log4j.LogManager;
//import org.apache.logging.log4j.Logger;
//
//import javax.annotation.concurrent.ThreadSafe;
//import java.util.HashMap;
//import java.util.Map;
//
///**
// * @author zhongrenli
// */
//@ThreadSafe
//public class RecallScoreWeightDataSource extends BaseDataSource {
//	private static Logger logger = LogManager.getLogger(RecallScoreWeightDataSource.class);
//
//	/**
//	 * recallName -> recallScore
//	 */
//	private volatile Map<String, Float> recallScoreWeightMap;
//
//	/**
//	 * 构造函数
//	 *
//	 * @param
//	 */
//	public RecallScoreWeightDataSource() {
//		init();
//	}
//
//	/**
//	 * 初始化
//	 *
//	 * @param
//	 */
//	public void init() {
//		logger.info("{\"dataSourceInfo\":\"[RecallScoreWeightDataSource init successfully]\"}");
//	}
//
//	private void fillRecallScoreWeight() {
//		try {
//			Map<String, IStreamComponent> recallMap = RecallManager.INSTANCE.getRecallMap();
//			if (MXJudgeUtils.isEmpty(recallMap)) {
//				return;
//			}
//			recallScoreWeightMap = new HashMap<>();
//			for (Map.Entry<String, IStreamComponent> entry : recallMap.entrySet()) {
//				recallScoreWeightMap.put(entry.getKey(), ((IRecall) entry.getValue()).getRecallWeightScore());
//			}
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//	}
//
//	public Map<String, Float> getRecallScoreWeightMap() {
//		if (null == recallScoreWeightMap) {
//			synchronized (RecallScoreWeightDataSource.class){
//				if (null == recallScoreWeightMap) {
//					fillRecallScoreWeight();
//				}
//			}
//		}
//		return recallScoreWeightMap;
//	}
//}
