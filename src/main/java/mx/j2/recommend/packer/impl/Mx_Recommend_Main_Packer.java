package mx.j2.recommend.packer.impl;

import com.google.common.collect.Lists;
import com.newrelic.api.agent.Trace;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.data_collection.FeedDataCollection;
import mx.j2.recommend.data_model.data_collection.info.MXEntityDebugInfo;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_model.document.ScoreDocument;
import mx.j2.recommend.data_model.document.ShortDocument;
import mx.j2.recommend.data_model.document.StatisticsDocument;
import mx.j2.recommend.data_source.UserProfileDataSource;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.manager.MXManager;
import mx.j2.recommend.recall.impl.BaseRecall;
import mx.j2.recommend.server.RecommendServer;
import mx.j2.recommend.thrift.InternalUse;
import mx.j2.recommend.thrift.Result;
import mx.j2.recommend.thrift.ShortVideo;
import mx.j2.recommend.thrift.ThumbnailInfo;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXJudgeUtils;
import mx.j2.recommend.util.MXStringUtils;
import mx.j2.recommend.util.OptionalUtil;
import org.apache.commons.lang.StringUtils;

import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author zhongren.li
 * @date 2018/4/19
 */
public class Mx_Recommend_Main_Packer extends BasePacker {

    @Override
    @Trace(dispatcher = true)
    public void pack(BaseDataCollection dc) {
        for (BaseDocument doc : dc.highPriorityManualList) {
            if (doc == null) {
                continue;
            }
            packResult(dc, doc, dc.highPriorityManualResultList);
        }

        if (UserProfileDataSource.isPureNewUser(dc)) {
            for (BaseDocument doc : dc.highPriorityVideoForNewUserList) {
                if (doc == null) {
                    continue;
                }
                packResult(dc, doc, dc.highPriorityVideoForNewUserResultList);
            }
        }

        for (BaseDocument doc : dc.data.temp.mixDocumentList) {
            if (doc == null) {
                continue;
            }
            packResult(dc, doc, dc.data.result.resultList);
        }

        for (BaseDocument doc : dc.fallbackList) {
            if (doc == null) {
                continue;
            }
            packResult(dc, doc, dc.data.result.resultList);
        }
    }

    private Result packResult(BaseDataCollection dc, BaseDocument doc, List<Result> resultList) {
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

            ThumbnailInfo thumbnailInfo = new ThumbnailInfo();
            thumbnailInfo.setThumbnailUrl(shortdoc.thumbnailUrl);
            thumbnailInfo.setWidth(shortdoc.thumbnailWidth);
            thumbnailInfo.setHeight(shortdoc.thumbnailHeight);
            shortVideo.setThumbnailInfo(thumbnailInfo);

            r.setShortVideo(shortVideo);

            if (shortdoc.isUgc) {
                dc.userUploadVideoIDList.add(shortdoc.id);
            }

            if (shortdoc.isBigV) {
                dc.bigVPublishVideoIdList.add(shortdoc.id);
            }
            dc.resulIdLanMap.put(shortdoc.id, shortdoc.languageId);

            // 文档调试信息
            MXEntityDebugInfo entityDebugInfo = dc.debug.getDebugInfoByEntityId(doc.id);

            if (dc.isDebugModeOpen) {
                String debugInfo = packDebugInfo(dc, shortdoc, entityDebugInfo).toString();
                dc.debug.debugInfoMap.put(shortdoc.id, debugInfo);
                r.setDebugInfo(debugInfo);
            }

            String attachContent = packAttachContent(dc, shortdoc, entityDebugInfo).toString();
            r.setAttachContent(attachContent);
            dc.debug.attachInfoMap.put(shortdoc.id, attachContent);

            InternalUse internalUse = new InternalUse();
            if (shortdoc.isTopHotHistory()) {
                internalUse.setIsTophot(1);
            } else {
                internalUse.setIsTophot(0);
            }
            internalUse.setAppName(shortdoc.appName);
            internalUse.setVideoSource(shortdoc.videoSource);
            internalUse.setPublisherId(shortdoc.publisher_id);
            internalUse.setScore(shortdoc.scoreDocument.toString());
            internalUse.setRecallName(getRecallName(shortdoc, entityDebugInfo));
            internalUse.setRecallResultID(entityDebugInfo.recall.result);
            internalUse.setSmallFlowName(dc.recommendFlow.name);
            internalUse.setIsBigV(shortdoc.isBigV);
            internalUse.setIsUgc(shortdoc.isUgc);
            internalUse.setHeatScore2(shortdoc.heatScore2);
            internalUse.setPoolLevel(doc.getPoolLevel());
            internalUse.setPoolPriority(doc.getPoolPriority());
            /*如国家为空，默认为IND*/
            if (MXJudgeUtils.isEmpty(shortdoc.countries)) {
                internalUse.setCountries(Lists.newArrayList("IND"));
            } else {
                internalUse.setCountries(shortdoc.countries);
            }
            r.setInternalUse(internalUse);

            resultList.add(r);
            return r;
        }
        return null;
    }

    private StringBuilder packDebugInfo(BaseDataCollection dc, BaseDocument document, MXEntityDebugInfo entityDebugInfo) {
        StringBuilder debugInfo = new StringBuilder();
        // TODO-ZXJ 因为性能问题暂时下掉
        //RealTimeStrategyDataSource dataSource = DataSourceManager.INSTANCE.getRealTimeStrategyDataSource();
        //dataSource.setExtraInfo(document);
        //debugInfo.append(document.scoreDocument.toString());暂时下掉

        debugInfo.append("recall: ");
        if (MXStringUtils.isNotEmpty(document.poolLevel) || MXStringUtils.isNotEmpty(document.poolIndex)) {
            debugInfo.append(String.format("%s %s_%s", getRecallName(document, entityDebugInfo), document.poolIndex, document.poolLevel));
        } else {
            debugInfo.append(getRecallName(document, entityDebugInfo));
        }
        debugInfo.append(",");

        debugInfo.append("\nis_follow_pool: ").append(document.isFollowed).append(",");// 是否为follow置顶

        // 扶持账号等级
        debugInfo.append("\nsupport-level: ").append(MXDataSource.white().getSupportLevel(document.publisher_id)).append(",");

        appendRecallInfo(debugInfo, document, entityDebugInfo);

        debugInfo.append("\nml_tag: ").append(document.mlTagToString()).append(",");
        debugInfo.append("\np_tags: ").append(document.primaryTags).append(",");
        debugInfo.append("\ns_tags: ").append(document.secondaryTags).append(",");
        debugInfo.append("\napp: ").append(document.appName).append(",");
        if(MXStringUtils.isNotEmpty(document.postFrom)) {
            debugInfo.append("\npost_from: ").append(document.postFrom).append(",");
        }

        String sfName = dc.recommendFlow.name;
        if (sfName.contains("mx_hot_tab_internal_version_2_0_")) {
            sfName = sfName.replace("mx_hot_tab_internal_version_2_0_", "");
        }
        debugInfo.append("\nsmall_flow: ").append(sfName).append(",");

        debugInfo.append("\nlang: ").append(dc.req.languageList).append(",");
        debugInfo.append("\npid: ").append(document.publisher_id).append(",");
        debugInfo.append("\nhost: ").append(RecommendServer.HostIp).append(",");
        debugInfo.append("\nduration: ").append(document.getDuration() / 1000).append(",");

        OptionalUtil.ofNullable(document.statisticsDocument)
                .getUtil(StatisticsDocument::getDownloadRate30d)
                .ifPresent(rate -> {
                    if (rate > 0) {
                        debugInfo.append("\ndownload: ").append(new DecimalFormat("###.00").format(rate * 100)).append("% | ")
                                .append(new DecimalFormat("###.00").format(document.statisticsDocument.getDownloadRateRealtime() * 100)).append("% | ")
                                .append(new DecimalFormat("###.00").format(document.statisticsDocument.getDownloadRateESPool() * 100)).append("% , ");
                    }
                });
        OptionalUtil.ofNullable(document.statisticsDocument)
                .getUtil(StatisticsDocument::getLikeRate30d)
                .ifPresent(rate -> {
                    if (rate > 0) {
                        debugInfo.append("\nlike: ").append(new DecimalFormat("###.00").format(rate * 100)).append("% | ")
                                .append(new DecimalFormat("###.00").format(document.statisticsDocument.getLikeRateRealtime() * 100)).append("% | ")
                                .append(new DecimalFormat("###.00").format(document.statisticsDocument.getLikeRateESPool() * 100)).append("% , ");
                    }
                });
        OptionalUtil.ofNullable(document.statisticsDocument)
                .getUtil(StatisticsDocument::getFinishedRate30d)
                .ifPresent(rate -> {
                    if (rate > 0) {
                        debugInfo.append("\nfinish: ").append(new DecimalFormat("###.00").format(rate * 100)).append("% | ")
                                .append(new DecimalFormat("###.00").format(document.statisticsDocument.getFinishedRateRealtime() * 100)).append("% | ")
                                .append(new DecimalFormat("###.00").format(document.statisticsDocument.getFinishedRateESPool() * 100)).append("% , ");
                    }
                });
        OptionalUtil.ofNullable(document.statisticsDocument)
                .getUtil(StatisticsDocument::getShareRate30d)
                .ifPresent(rate -> {
                    if (rate > 0) {
                        debugInfo.append("\nshare: ").append(new DecimalFormat("###.00").format(rate * 100)).append("% | ")
                                .append(new DecimalFormat("###.00").format(document.statisticsDocument.getShareRateRealtime() * 100)).append("% | ")
                                .append(new DecimalFormat("###.00").format(document.statisticsDocument.getShareRateESPool() * 100)).append("% , ");
                    }
                });
        OptionalUtil.ofNullable(document.statisticsDocument)
                .getUtil(StatisticsDocument::getPlayRate30d)
                .ifPresent(rate -> {
                    if (rate > 0) {
                        debugInfo.append("\nplay: ").append(new DecimalFormat("###.00").format(rate * 100)).append("% | ")
                                .append(new DecimalFormat("###.00").format(document.statisticsDocument.getPlayRateRealtime() * 100)).append("% | ")
                                .append(new DecimalFormat("###.00").format(document.statisticsDocument.getPlayRateESPool() * 100)).append("% , ");
                    }
                });
        OptionalUtil.ofNullable(document.statisticsDocument)
                .getUtil(StatisticsDocument::getFinishRetentionSum10s30d)
                .ifPresent(rate -> {
                    if (rate > 0) {
                        debugInfo.append("\nfinish_RS_10s: ").append(new DecimalFormat("###.00").format(rate)).append(", ");
                    }
                });
        OptionalUtil.ofNullable(document.statisticsDocument)
                .getUtil(StatisticsDocument::getViewAll30d)
                .ifPresent(rate -> {
                    if (rate > 0) {
                        debugInfo.append("\nview_all: ").append(rate).append(" | ")
                                .append(document.statisticsDocument.getViewAllRealtime()).append(" | ")
                                .append(document.statisticsDocument.getViewAllESPool()).append(", ");
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

        // 视频原始尺寸
        debugInfo.append("\nheight: ").append(document.getOriginalHeight()).append(",");
        debugInfo.append("\nwidth: ").append(document.getOriginalWidth()).append(",");

        return debugInfo;
    }

    /**
     * 追加召回信息
     */
    private void appendRecallInfo(StringBuilder outDebugInfo, BaseDocument document, MXEntityDebugInfo debugInfo) {
        BaseRecall recall = MXManager.recall().getComponentInstance(getRecallName(document, debugInfo));

        // 注意，现在启动时为每个类都生成了一个实例，不管是不是配置的，所以此处都能拿到实例
        if (recall != null) {
            Map<String, String> infoMap = new HashMap<>();
            recall.fillDebugInfo(infoMap, document);

            if (MXJudgeUtils.isNotEmpty(infoMap)) {
                for (Map.Entry<String, String> entry : infoMap.entrySet()) {
                    outDebugInfo.append("\n").append(entry.getKey()).append(": ").append(entry.getValue()).append(",");
                }
            }
        }
    }

    private StringBuilder packAttachContent(BaseDataCollection dc, BaseDocument document, MXEntityDebugInfo debugInfo) {
        StringBuilder attachContent = new StringBuilder();

        // recall_name
        attachContent.append("{\"recall_name\":\"").append(getRecallName(document, debugInfo)).append("\",");
        // poolindex and poollv
        if (MXStringUtils.isNotEmpty(document.poolIndex) && MXStringUtils.isNotEmpty(document.poolLevel)) {
            attachContent.append("\"pool_index\":\"").append(document.poolIndex).append("\",");
            attachContent.append("\"pool_level\":\"").append(document.poolLevel).append("\",");
        }
        // tagPoolSmallFlow
        if (dc instanceof FeedDataCollection) {
            FeedDataCollection fdc = (FeedDataCollection) dc;
            if (MXStringUtils.isNotEmpty(fdc.tagPoolSmallFlow)) {
                attachContent.append("\"tagPoolFlow\":\"").append(fdc.tagPoolSmallFlow).append("\",");
            } else {
                attachContent.append("\"tagPoolFlow\":\"").append("notag").append("\",");
            }
        }
        // timestamp
        attachContent.append("\"timestamp\":").append(dc.startTime).append(",");

        if (MXJudgeUtils.isNotEmpty(document.getRetainTags())) {
            attachContent.append("\"retainTags\":\"").append(StringUtils.join(document.getRetainTags().toArray(), "_")).append("\",");
        }

        // 源视频Id
        if (MXJudgeUtils.isNotEmpty(debugInfo.recall.sourceId)) {
            attachContent.append("\"sourceVideoId\": \"").append(debugInfo.recall.sourceId).append("\",");
        }

        // small_flow_name
        attachContent.append("\"small_flow_name\":\"").append(dc.recommendFlow.name).append("\"}");

        return attachContent;
    }

    /**
     * 兼容新老版本获取方式
     */
    private String getRecallName(BaseDocument document, MXEntityDebugInfo debugInfo) {
        return MXJudgeUtils.isNotEmpty(debugInfo.recall.name) ? debugInfo.recall.name : document.recallName;
    }
}
