package mx.j2.recommend.scorer;

public interface IScorer<T> {

	/**
	 * 算法打分 接口
	 * 
	 * @param dc
	 *            一次请求的数据集合
	 */
	void score(T dc);
}
