package mx.j2.recommend.packer.impl;

import mx.j2.recommend.component.stream.base.BaseStreamComponent;
import mx.j2.recommend.data_model.Document;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.packer.IPacker;
import mx.j2.recommend.thrift.Result;


/**
 * 打包器，这里主要是组建前端需要展示的ui上面的元素
 *
 * @author zhuowei
 */
public abstract class BasePacker<T extends BaseDataCollection> extends BaseStreamComponent<T> implements IPacker<T> {

	public BasePacker() {

	}

	/**
	 * 子类必须实现该方法，是对每一个iterm进行打包
	 *
	 * @param doc
	 * @return
	 */
	public Result packOneResult(Document doc) {
		return new Result();
	}

	public void init() {}

	@Override
	public boolean skip(T data) {
		return false;
	}

	@Override
	public void doWork(T dc) {
		pack(dc);
	}

	@Override
	public void pack(T dc) {
		for (int i = 0; i < dc.mergedList.size(); i++) {
			Result r = packOneResult(dc.mergedList.get(i));
			if (null != r) {
				dc.data.result.resultList.add(r);
			}
		}
	}
}
