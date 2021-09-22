package mx.j2.recommend.packer.impl;

import mx.j2.recommend.data_model.data_collection.InternalDataCollection;
import mx.j2.recommend.data_model.data_collection.OtherDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.thrift.InternalResult;
import mx.j2.recommend.thrift.InternalShortVideo;
import mx.j2.recommend.util.DefineTool;

import java.util.List;
import java.util.stream.Collectors;

public class Mx_Recommend_Internal_Packer extends InternalBasePacker {
    @Override
    public void pack(InternalDataCollection dc, OtherDataCollection otherDc) {
        if (dc.internalReq.interfaceName.equals(DefineTool.FlowInterface.INTERNAL_SORTED_VIDEO_LIST_OF_PUBLISHER_1_0.getName())) {
            dc.internalResultList.add(packOneResult(dc, otherDc));
        } else if (dc.internalReq.interfaceName.equals(DefineTool.FlowInterface.INTERNAL_GENERAL_FILTER_1_0.getName())) {
            for (BaseDocument doc : otherDc.mergedList) {
                InternalResult r = packResult(dc, doc);
                dc.internalResultList.add(r);
            }
        } else if (dc.internalReq.interfaceName.equals(DefineTool.FlowInterface.INTERNAL_POOL_VIDEO_FILTER_1_0.getName())) {
            dc.internalResultList.add(packOneResultWithFilterInfo(dc, otherDc));
        } else if (dc.internalReq.interfaceName.equals(DefineTool.FlowInterface.INTERNAL_VIDEOS_OF_THE_TAG_VERSION_1_0.getName())) {
            dc.internalResultList.add(packOneResultTagVideo(dc, otherDc));
        }
    }

    private InternalResult packResult(InternalDataCollection dc, BaseDocument doc) {
        InternalResult internalResult = new InternalResult();
        internalResult.setPublisherId(doc.publisher_id);
        internalResult.setNum(0);
        InternalShortVideo internalShortVideo = new InternalShortVideo();
        internalShortVideo.setIsDuplicated(String.valueOf(doc.duplicated));
        internalShortVideo.setDuration(String.valueOf(doc.innerDuration));
        internalShortVideo.setMlTags(doc.mlTagsForInternal);
        internalShortVideo.setBigHead(doc.bigHeadForInternal);
        internalShortVideo.setFeatStat30d(doc.featStat30dForInternal);
        internalShortVideo.setLikeInfo(doc.likeInfoForInternal);
        internalShortVideo.setFeatStat0d(doc.featStat0dForInternal);
        internalShortVideo.setIsUgcContent(doc.isUgc);
        internalShortVideo.setIsDelogo(doc.waterMark);
        internalShortVideo.setId(doc.id);
        internalResult.setInternalShortVideo(internalShortVideo);
        return internalResult;
    }

    private InternalResult packOneResult(InternalDataCollection dc, OtherDataCollection otherDc) {
        InternalResult internalResult = new InternalResult();
        List<String> videoIds = otherDc.mergedList.stream().map(BaseDocument::getId).collect(Collectors.toList());
        internalResult.setNum(videoIds.size());
        internalResult.setVideoIdList(videoIds);
        internalResult.setPublisherId(dc.internalReq.resourceIdList.get(0));
        return internalResult;
    }

    private InternalResult packOneResultWithFilterInfo(InternalDataCollection dc, OtherDataCollection otherDc) {
        InternalResult internalResult = new InternalResult();
        InternalShortVideo internalShortVideo = new InternalShortVideo();
        if (otherDc.mergedList.size() > 0) {
            BaseDocument doc = otherDc.mergedList.get(0);
            internalResult.setPublisherId(doc.publisher_id);
            internalResult.setNum(0);
            internalShortVideo.setIsDuplicated(String.valueOf(doc.duplicated));
            internalShortVideo.setDuration(String.valueOf(doc.duration));
            internalShortVideo.setMlTags(doc.mlTagsForInternal);
            internalShortVideo.setBigHead(doc.bigHeadForInternal);
            internalShortVideo.setFeatStat30d(doc.featStat30dForInternal);
            internalShortVideo.setLikeInfo(doc.likeInfoForInternal);
            internalShortVideo.setId(doc.id);
        }
        internalResult.setInternalShortVideo(internalShortVideo);
        internalResult.setFilterInfo(otherDc.debug.deletedRecordMap.toString());
        return internalResult;
    }

    private InternalResult packOneResultTagVideo(InternalDataCollection dc, OtherDataCollection otherDc) {
        InternalResult internalResult = new InternalResult();
        internalResult.setNum(otherDc.totalNumber);
        List<BaseDocument> otherMergedList = otherDc.mergedList;
        for (BaseDocument document : otherMergedList) {
            internalResult.addToVideoIdList(document.id);
        }
        return internalResult;
    }
}