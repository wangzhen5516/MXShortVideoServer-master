package mx.j2.recommend.recall.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;

public class LongTermTagPoolRecall extends StrategyTagPoolRecall{

    @Override
    protected void setTagTable(BaseDataCollection dc) {
        dc.tagTableName = "up_human_tag_7d_v2";
    }
}
