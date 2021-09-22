package mx.j2.recommend.packer.impl;

import mx.j2.recommend.data_model.Document;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.data_collection.InternalDataCollection;
import mx.j2.recommend.data_model.data_collection.OtherDataCollection;
import mx.j2.recommend.packer.InternalPacker;
import mx.j2.recommend.thrift.InternalResult;
import mx.j2.recommend.thrift.Result;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class InternalBasePacker implements InternalPacker {
    private static Logger log = LogManager.getLogger(InternalBasePacker.class);

    public InternalBasePacker() {

    }

    /**
     * 子类必须实现该方法，是对每一个iterm进行打包
     */
    public InternalResult packOneResult(Document doc) {
        return new InternalResult();
    }

    @Override
    public void pack(InternalDataCollection dc, OtherDataCollection otherDc) {
        dc.moduleStartTime = System.nanoTime();
        for (int i = 0; i < dc.mergedList.size(); i++) {
            InternalResult r = packOneResult(dc.mergedList.get(i));
            if (null != r) {
                dc.internalResultList.add(r);
            }
        }
        dc.moduleEndTime = System.nanoTime();
        dc.appendToTimeRecord(dc.moduleEndTime - dc.moduleStartTime, this.getName());
    }

    @Override
    public String getName() {
        return this.getClass().getSimpleName();
    }
}
