package mx.j2.recommend.scorer.impl;

import mx.j2.recommend.data_model.UserProfile;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_source.UserStrategyTagDataSource;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.MXCollectionUtils;

import java.util.*;

/**
 * @Author: xiaoling.zhu
 * @Date: 2021-03-15
 */

public class TagScorer extends BaseScorer<BaseDataCollection> {
    private static final int SCORE_NUM = 15;

    @Override
    public boolean skip(BaseDataCollection data) {
        return false;
    }

    @Override
    public void score(BaseDataCollection dc) {
        try {
            if(MXCollectionUtils.isEmpty(dc.userLongTagSet)){
                return;
            }
            Set<String> tagsG3 = new HashSet<>();
            UserStrategyTagDataSource dataSource = MXDataSource.profileTagV2();
            dc.tagTableName = "up_ml_tag_60d_v1";
            List<UserProfile.Tag> tags = dataSource.getTags(dc);
            tags.stream().forEach(tag -> {
                if(tag.score>=3){
                    tagsG3.add(tag.name.trim());
                }
            });
            for(Map.Entry<String, List<BaseDocument> > entry : dc.poolToDocumentListMap.entrySet()){
                String key = entry.getKey();
                key = key.replace("taka_flowpool_lv", "");
                try {
                    String[] key_item = key.split("_");
                    //System.out.println(key_item[0]);
                    int lv = Integer.parseInt(key_item[0]);
                    if(lv <= 6) {
                        continue;
                    }
                }catch(Exception ex) {
                }
                List<BaseDocument> documents = entry.getValue();
                List<BaseDocument> toHead = new ArrayList<>();
                int i = 0;
                for (int j = 0;( j < documents.size() ) && (i<SCORE_NUM); j++) {
                    BaseDocument d = documents.get(j);
                    boolean temp = false;
                    for (String tag : d.mlTags){
                        if(tagsG3.contains(tag)){
                            toHead.add(d);
                            i++;
                            temp = true;
                            break;
                        }
                    }
                    if(temp){
                        continue;
                    }
                    for (int i1 = 0; i1 < d.primaryTags.size(); i1++) {
                        String tag = d.primaryTags.getString(i1);
                        if(tagsG3.contains(tag)){
                            toHead.add(d);
                            i++;
                            temp = true;
                            break;
                        }
                    }
                    if(temp){
                        continue;
                    }
                    for (int i1 = 0; i1 < d.secondaryTags.size(); i1++) {
                        String tag = d.secondaryTags.getString(i1);
                        if(tagsG3.contains(tag)){
                            toHead.add(d);
                            i++;
                            temp = true;
                            break;
                        }
                    }
                    if(temp){
                        continue;
                    }
                }
                if(MXCollectionUtils.isNotEmpty(toHead)){
                    documents.removeAll(toHead);
                    documents.addAll(toHead);
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
