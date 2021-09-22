package mx.j2.recommend.packer.impl;

import com.newrelic.api.agent.Trace;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.data_collection.FetchTabsDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_model.document.CardListItemDocument;
import mx.j2.recommend.thrift.Card;
import mx.j2.recommend.thrift.InternalUse;


public class MX_Recommend_Tab_NewPacker extends BasePacker {
    @Override
    @Trace(dispatcher = true)
    public void pack(BaseDataCollection baseDc) {
        if (!(baseDc instanceof FetchTabsDataCollection)) {
            return;
        }
        FetchTabsDataCollection dc = (FetchTabsDataCollection) baseDc;
        for (BaseDocument doc : dc.mergedList) {
            if (doc instanceof CardListItemDocument) {
                InternalUse internalUse = new InternalUse();
                dc.cardList.add(new Card(doc.getId()));
            }
        }
    }
}
