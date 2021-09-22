package mx.j2.recommend.ranker;

/**
 * 排序器接口
 * 
 * @author zhuowei
 *
 */
public interface IRanker<T> {

	/**
	 * 排序器接口
	 * 
	 * @param dc
	 *            一次请求的数据总集
	 */
	void rank(T dc);
}
