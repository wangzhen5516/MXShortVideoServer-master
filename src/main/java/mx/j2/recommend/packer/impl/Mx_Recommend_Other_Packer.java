package mx.j2.recommend.packer.impl;

import com.google.common.collect.Lists;
import com.newrelic.api.agent.Trace;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_model.document.ScoreDocument;
import mx.j2.recommend.data_model.document.ShortDocument;
import mx.j2.recommend.data_model.statistics_document.BaseStatisticsDocument;
import mx.j2.recommend.data_model.document.StatisticsDocument;
import mx.j2.recommend.server.RecommendServer;
import mx.j2.recommend.thrift.InternalUse;
import mx.j2.recommend.thrift.Result;
import mx.j2.recommend.thrift.ShortVideo;
import mx.j2.recommend.thrift.ThumbnailInfo;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXJudgeUtils;
import mx.j2.recommend.util.OptionalUtil;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

import static mx.j2.recommend.util.BaseMagicValueEnum.FEATURE30D;

/**
 * @author zhongren.li
 * @date 2018/4/19
 */
public class Mx_Recommend_Other_Packer extends BasePacker {

    @Override
    @Trace(dispatcher = true)
    public void pack(BaseDataCollection dc) {
        for (int i = 0; i < dc.mergedList.size(); i++) {
            BaseDocument doc = dc.mergedList.get(i);

            if (doc == null) {
                continue;
            }
            Result r = packResult(dc, doc, dc.data.result.resultList);
            addToScoreMap(dc, doc, r);
        }
        // 脏逻辑，统计该音频下的视频总数
        if (DefineTool.FlowInterface.MX_VIDEOS_OF_THE_SAME_AUDIO_VERSION_1_0.getName().equals(dc.req.interfaceName)) {
            dc.data.response.setResultNum(dc.totalNumber);
        } else if (DefineTool.FlowInterface.MX_CREATOR_CHECK_VERSION_1_0.getName().equals(dc.req.interfaceName)) {
            // 脏逻辑，返回给定天数内，该用户上传视频的天数
            dc.data.response.setResultNum(dc.uploadDaysRecent);
        }

    }

    public static Result packResult(BaseDataCollection dc, BaseDocument doc, List<Result> resultList) {
        Result r = new Result();

        r.setResultType(doc.category.getName());
        r.setId(doc.id);

        if (DefineTool.CategoryEnum.SHORT_VIDEO.equals(doc.category)) {
            ShortVideo shortVideo = new ShortVideo();
            ShortDocument shortdoc = (ShortDocument) doc;
            shortVideo.setId(shortdoc.id);
            shortVideo.setType(shortdoc.itemType);
            shortVideo.setDescription(shortdoc.description);
            shortVideo.setViewCount(shortdoc.viewCount);
            shortVideo.setLikeCount(shortdoc.likeCount);
            shortVideo.setWShareCount(shortdoc.shareCount);
            shortVideo.setDownCount(shortdoc.downloadCount);
            shortVideo.setContentUrl(shortdoc.contentUrl);
            shortVideo.setName(shortdoc.title);
            shortVideo.setDownLoadUrl(shortdoc.downloadUrl);
            shortVideo.setDistance(shortdoc.distance);

            ThumbnailInfo thumbnailInfo = new ThumbnailInfo();
            thumbnailInfo.setThumbnailUrl(shortdoc.thumbnailUrl);
            thumbnailInfo.setWidth(shortdoc.thumbnailWidth);
            thumbnailInfo.setHeight(shortdoc.thumbnailHeight);
            shortVideo.setThumbnailInfo(thumbnailInfo);

            r.setShortVideo(shortVideo);

            if (shortdoc.isUgc) {
                dc.userUploadVideoIDList.add(shortdoc.id);
            }
            dc.resulIdLanMap.put(shortdoc.id, shortdoc.languageId);

            if (dc.isDebugModeOpen) {
                String debugInfo = packDebugInfo(dc, shortdoc).toString();
                dc.debug.debugInfoMap.put(shortdoc.id, debugInfo);
                r.setDebugInfo(debugInfo);
            }


            String attachContent = packAttachContent(dc, shortdoc).toString();
            r.setAttachContent(attachContent);
            dc.debug.attachInfoMap.put(shortdoc.id, attachContent);

            InternalUse internalUse = new InternalUse();
            internalUse.setAppName(shortdoc.appName);
            internalUse.setVideoSource(shortdoc.videoSource);
            internalUse.setPublisherId(shortdoc.publisher_id);
            internalUse.setScore(shortdoc.scoreDocument.toString());
            internalUse.setOnlineTime(shortdoc.onlineTime);
            internalUse.setOrder(shortdoc.order);
            internalUse.setHeatScore2(shortdoc.heatScore);
            internalUse.setRecallName(shortdoc.recallName);
            internalUse.setSmallFlowName(dc.recommendFlow.name);
            internalUse.setHashtagHeat(shortdoc.hashtagHeat);
            internalUse.setMultipleScore(shortdoc.multipleScore);
            internalUse.setOnlineTimeNeed(shortdoc.onlineTimeNeed);
            internalUse.setFinishRetentionSum10s30d(shortdoc.statisticsDocument.getFinishRetentionSum10s30d());
            internalUse.setScore_30d(shortdoc.statisticsDocument.getScore_30d());
            internalUse.setNextToken(shortdoc.nextTokenMap.get(dc.req.interfaceName));
            /*如国家为空，默认为IND*/
            if (MXJudgeUtils.isEmpty(shortdoc.countries)) {
                internalUse.setCountries(Lists.newArrayList("IND"));
            } else {
                internalUse.setCountries(shortdoc.countries);
            }
            r.setInternalUse(internalUse);
        }

        if (resultList != null) {
            resultList.add(r);
        }

        return r;
    }

    private static StringBuilder packDebugInfo(BaseDataCollection dc, BaseDocument document) {
        StringBuilder debugInfo = new StringBuilder();
        //debugInfo.append(document. .toString());
        debugInfo.append("recall: ").append(document.recallName).append(",");
        debugInfo.append("\nml_tag: ").append(document.mlTagToString()).append(",");
        debugInfo.append("\np_tags: ").append(document.primaryTags).append(",");
        debugInfo.append("\ns_tags: ").append(document.secondaryTags).append(",");
        debugInfo.append("\nsf: ").append(dc.recommendFlow.name).append(",");
        debugInfo.append("\npid: ").append(document.publisher_id).append(",");
        debugInfo.append("\nhost: ").append(RecommendServer.HostIp).append(",");

        debugInfo.append("\nduration: ").append(document.getDuration() / 1000).append(",");

        OptionalUtil.ofNullable(document.statisticsDocument.get(FEATURE30D))
                .getUtil(BaseStatisticsDocument::getDownloadRate)
                .ifPresent(rate -> {
                    if (rate > 0) {
                        debugInfo.append("\ndownload_30d: ").append(new DecimalFormat("###.0000").format(rate * 100)).append("%,");
                    }
                });

        OptionalUtil.ofNullable(document.statisticsDocument.get(FEATURE30D))
                .getUtil(BaseStatisticsDocument::getLikeRate)
                .ifPresent(rate -> {
                    if (rate > 0) {
                        debugInfo.append("\nlike_30d: ").append(new DecimalFormat("###.0000").format(rate * 100)).append("%,");
                    }
                });
        OptionalUtil.ofNullable(document.statisticsDocument.get(FEATURE30D))
                .getUtil(BaseStatisticsDocument::getFinishRate)
                .ifPresent(rate -> {
                    if (rate > 0) {
                        debugInfo.append("\nfinish_30d: ").append(new DecimalFormat("###.0000").format(rate * 100)).append("%,");
                    }
                });
        OptionalUtil.ofNullable(document.statisticsDocument.get(FEATURE30D))
                .getUtil(BaseStatisticsDocument::getShareRate)
                .ifPresent(rate -> {
                    if (rate > 0) {
                        debugInfo.append("\nshare_30d: ").append(new DecimalFormat("###.0000").format(rate * 100)).append("%,");
                    }
                });
        OptionalUtil.ofNullable(document.statisticsDocument.get(FEATURE30D))
                .getUtil(BaseStatisticsDocument::getPlayRate)
                .ifPresent(rate -> {
                    if (rate > 0) {
                        debugInfo.append("\nplay_30d: ").append(new DecimalFormat("###.0000").format(rate * 100)).append("%,");
                    }
                });
        OptionalUtil.ofNullable(document.statisticsDocument.get(FEATURE30D))
                .getUtil(BaseStatisticsDocument::getFinishRetentionSum10s)
                .ifPresent(rate -> {
                    if (rate > 0) {
                        debugInfo.append("\nfinish_RS_10s30d: ").append(new DecimalFormat("###.0000").format(rate)).append(",");
                    }
                });
        OptionalUtil.ofNullable(document.statisticsDocument.get(FEATURE30D))
                .getUtil(BaseStatisticsDocument::getViewAll)
                .ifPresent(rate -> {
                    if (rate > 0) {
                        debugInfo.append("\nview_all_30d: ").append(rate).append(",");
                    }
                });

        OptionalUtil.ofNullable(document.statisticsDocument)
                .getUtil(StatisticsDocument::getAvgPlaytime30d)
                .ifPresent(rate -> {
                    if (rate > 0) {
                        debugInfo.append("\navg_play_time: ").append(rate).append(" , ");
                    }
                });

        OptionalUtil.ofNullable(document.scoreDocument)
                .getUtil(ScoreDocument::getPredictScore)
                .ifPresent(score -> {
                    if (score > 0) {
                        debugInfo.append("\npredict_score: ").append(score).append(",");
                    }
                });

        return debugInfo;
    }

    /**
     * 生成返回给客户端的埋点数据
     *
     * @param dc
     * @param document
     * @return
     */
    private static StringBuilder packAttachContent(BaseDataCollection dc, BaseDocument document) {
        StringBuilder attachContent = new StringBuilder();

        // recall_name
        attachContent.append("{\"recall_name\":").append(document.recallName).append(",");

        // small_flow_name
        attachContent.append("\"small_flow_name\":\"").append(dc.recommendFlow.name).append("\"}");

        return attachContent;
    }

    private void addToScoreMap(BaseDataCollection dc, BaseDocument doc, Result result) {
        float score = doc.scoreDocument.baseScore;
        if (dc.scoreToResultListMap.containsKey(score)) {
            dc.scoreToResultListMap.get(score).add(result);
        } else {
            List<Result> resultList = new ArrayList<>();
            resultList.add(result);
            dc.scoreToResultListMap.put(score, resultList);
        }
        dc.videoIdToTagListMap.put(doc.id, doc.humanTagList);
    }
}
