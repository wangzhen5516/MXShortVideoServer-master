package mx.j2.recommend.ranker.impl;

import mx.j2.recommend.component.stream.base.BaseStreamComponent;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.ranker.IRanker;

/**
 * 排序器基类
 * 
 * @author zhuowei
 *
 */
public abstract class BaseRanker<T extends BaseDataCollection> extends BaseStreamComponent<T> implements IRanker<T> {

	/**
	 * 构造函数
	 */
	public BaseRanker() {}

	/**
	 * 子类需要实现的打分器初始化
	 * 
	 */
	public void init() {}

	@Override
	public void doWork(T dc) {
		rank(dc);
	}
}
