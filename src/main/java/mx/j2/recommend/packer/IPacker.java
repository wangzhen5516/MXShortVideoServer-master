package mx.j2.recommend.packer;

/**
 * 程序打包封装ui展现的接口
 * 
 * @author zhuowei
 *
 */
public interface IPacker<T> {

	/**
	 * 结果打包，根据ui需要装配打包元素
	 */
	void pack(T dc);
}
