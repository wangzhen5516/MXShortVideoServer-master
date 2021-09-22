package mx.j2.recommend.recall_data_in_mem;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.util.Map;

public interface ESCanGetStartRecall {
    int RECALL_WEIGHT = 1200;

    String getRecallName();

    float getRecallDocumentWeight();


    Map<String , BaseDataCollection.ESRequest> getESRequestMap();

    Map<String, SearchSourceBuilder> getSearchSourceBuilderMap();

    void doSomethingAfterLoad();

    int scheduledPeriodSeconds();

    int getRandomFactor();
}
