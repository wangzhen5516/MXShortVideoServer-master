package mx.j2.recommend.recall.impl;

import mx.j2.recommend.data_model.data_collection.OtherDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_source.LocalCacheDataSource;
import mx.j2.recommend.data_source.VideoElasticSearchDataSource;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.MXStringUtils;
import org.apache.commons.collections.CollectionUtils;

import java.util.*;

public class SameAudioManualTopRecall extends BaseRecall<OtherDataCollection> {
    private static Map<String, List<String>> specialAudioList = new HashMap<>();

    static {
        List<String> list2 = new ArrayList<>(Arrays.asList("20000nze8c","20000nf1ye","20000nvI03","20000nl77v","20000nl0j9","20000nkVjt","20000nkKN8","20000nlH9t","20000nlGv0","20000ngyZW","20000njQ41","20000niVtI","20000ni6LV","20000nhYhB"));
        List<String> list1 = new ArrayList<>(Arrays.asList("20000nxMPI","20000nwIZq","20000nwqoU","20000nvysW","20000nwhSO","20000nvWaK","20000nzdtA","20000nzaqQ","20000nyERm","20000ny4lq","20000nxshf","20000nxoj5","20000nwyBS","20000nvJW9"));
        specialAudioList.put("180023f3706c0bc3aee6ca5ac25556f813b02", list1);
        specialAudioList.put("18002a20475a0e2c991d6c31de05ad2343474", list2);
    }

    @Override
    public void recall(OtherDataCollection dc) {
        if (MXStringUtils.isNotEmpty(dc.req.nextToken) || !specialAudioList.keySet().contains(dc.req.resourceId)) {
            return;
        }
        LocalCacheDataSource local = MXDataSource.cache();
        List<BaseDocument>res = local.getSameAudioManualTopCache(dc.req.resourceId);
        if(CollectionUtils.isNotEmpty(res)){
            if(CollectionUtils.isEmpty(dc.mergedList)){
                dc.mergedList.addAll(res);
            }else{
                dc.mergedList.addAll(0,res);
            }
            return;
        }
        VideoElasticSearchDataSource videoElasticSearchDataSource = MXDataSource.videoES();
        List<String> idForSearch = specialAudioList.get(dc.req.resourceId);
        res = videoElasticSearchDataSource.getDetail(dc,idForSearch);
        if(CollectionUtils.isEmpty(res)){
            return;
        }
        local.setSameAudioManualTopCache(dc.req.resourceId,res);

        if(CollectionUtils.isEmpty(dc.mergedList)){
            dc.mergedList.addAll(res);
        }else{
            dc.mergedList.addAll(0,res);
        }
        return;

    }
}
