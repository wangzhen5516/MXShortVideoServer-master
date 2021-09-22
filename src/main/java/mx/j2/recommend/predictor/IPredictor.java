package mx.j2.recommend.predictor;

/**
 * 预测器接口
 * 
 * @author zhangxuejian
 *
 */
public interface IPredictor<T> {
	/**
	 * 预测器接口
	 * 
	 * @param dc
	 *            一次请求的数据总集
	 */
	void predict(T dc);
}
