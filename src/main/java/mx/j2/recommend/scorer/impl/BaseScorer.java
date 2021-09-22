package mx.j2.recommend.scorer.impl;

import mx.j2.recommend.component.stream.base.BaseStreamComponent;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.scorer.IScorer;

public abstract class BaseScorer<T extends BaseDataCollection> extends BaseStreamComponent<T> implements IScorer<T> {
	/**
	 * 构造函数
	 * 
	 * @param
	 */
	public BaseScorer() {}

	/**
	 * 打分器初始化
	 * 
	 * @param
	 */
	public void init(){}

	@Override
	public void doWork(T dc) {
		score(dc);
	}

	public static void main(String[] args){
	    Long s = 0L;
        System.out.println(Float.valueOf(s.toString()));
    }
}
