package mx.j2.recommend.ruler;

public interface IRuler<T> {

	/**
	 * 规则调序接口
	 * 
	 * @param dc
	 *            一次请求的数据集合
	 */
	void rule(T dc);
}
