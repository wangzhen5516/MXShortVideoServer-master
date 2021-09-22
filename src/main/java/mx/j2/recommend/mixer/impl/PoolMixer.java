//package mx.j2.recommend.mixer.impl;
//
//import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
//import mx.j2.recommend.data_model.document.BaseDocument;
//import mx.j2.recommend.pool_conf.PoolConf;
//import mx.j2.recommend.util.CollectionUtil;
//import mx.j2.recommend.util.DefineTool;
//import org.apache.commons.collections.CollectionUtils;
//
//import java.util.List;
//import java.util.Random;
//
///**
// * @author ：zhongrenli
// * @date ：Created in 5:08 下午 2020/8/19
// */
//@Deprecated
//public class PoolMixer extends BaseMixer{
//
//    @Override
//    public boolean skip(BaseDataCollection data) {
//        return CollectionUtils.isEmpty(data.poolConfList)
//                || CollectionUtil.isEmpty(data.poolToDocumentListMap);
//    }
//
//    @Override
//    public void mix(BaseDataCollection dc) {
//        Random random = new Random();// TODO: 建议优化
//        for (PoolConf pc : dc.poolConfList) {
//
//            if (!dc.poolToDocumentListMap.containsKey(pc.poolIndex)) {
//                continue;
//            }
//            List<BaseDocument> documentList = dc.poolToDocumentListMap.get(pc.poolIndex);
//            if (CollectionUtils.isEmpty(documentList)) {
//                continue;
//            }
//            double d = random.nextDouble();
//            if (Double.compare(d, pc.percentage) < 0) {
//                BaseDocument doc = documentList.get(0);
//                if (null == doc) {
//                    continue;
//                }
//                doc.recallPoolName = pc.poolIndex;
//                if (DefineTool.MixType.MIX.getType().equals(pc.poolLevel)) {
//                    dc.mixDocumentList.add(doc);
//                } else if (DefineTool.MixType.GUARANTEE.getType().equals(pc.poolLevel)) {
//                    dc.guaranteeFirstLevelDocList.add(doc);
//                }
//            }
//        }
//    }
//}
