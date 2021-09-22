package mx.j2.recommend.ruler.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;

public class PriorityRuler extends BaseRuler<BaseDataCollection> {
    @Override
    public boolean skip(BaseDataCollection data) {
        return false;
    }

    @Override
    public void rule(BaseDataCollection dc) {
        dc.data.result.resultList.sort((r0, r1) -> Integer.compare(r1.internalUse.poolPriority, r0.internalUse.poolPriority));
    }
}
