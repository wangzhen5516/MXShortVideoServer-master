package mx.j2.recommend.recall;

/**
 * 数据召回接口
 * 
 * @author zhuowei
 *
 */
public interface IRecall<T> {
	/**
	 * 数据召回接口
	 * 
	 * @param dc
	 *            一次请求的数据集合
	 */
	void recall(T dc);

	/**
	 * 获取recall 的权重
	 * @return recall score
	 */
	float getRecallWeightScore();
}
