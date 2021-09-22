package mx.j2.recommend.recall.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;

public class LongTermPubRecall extends RealTimePubRecall{
    //todo: 改下拉链
    @Override
    public void recall(BaseDataCollection dc) {

//        QUERY_FORMAT = "select %s from %s where uuid = '%s';";
//
//        COLUMN_NAME = "tag_profile";
//
//        TABLE = "up_publisher_30d_v1";
//
//        flag = 'l';

        doRecall(dc,dc.userPrePubDocLongTermList);
    }
}
