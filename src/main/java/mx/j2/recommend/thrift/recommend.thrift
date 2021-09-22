namespace java mx.j2.recommend.thrift  // defines the namespace

struct ThumbnailInfo{
 1: string thumbnailUrl,
 2: i32 width,
 3: i32 height,
}

struct UserInfo{
 1: string uuid,
 2: optional string userId,
 3: optional string adId,
}

struct InternalUse{
 1: optional string appName,
 2: optional string score,
 3: optional string videoSource,
 4: optional string publisherId,
 5: optional list<string> tags,
 6: optional i64 updateTime,
 7: optional i64 onlineTime,
 8: optional list<string> languageList,
 9: optional list<string> countries,
 10: optional double distance,
 11: optional string smallFlowName,
 12: optional i32 userCode,
 13: optional string recallName,
 14: optional double recallScore,
 15: optional i32 order,
 16: optional i32 heatScore,
 17: optional i32 isTophot,
 18: optional double heatScore2,
 19: optional bool isFromInfiniteHashtag,
 20: optional double multipleScore,
 21: optional double hashtagHeat,
 22: optional bool isBigV,
 23: optional i64 onlineTimeNeed,
 24: optional bool isUgc,
 25: optional string poolLevel,
 26: optional double finishRetentionSum10s30d,
 27: optional i32 poolPriority,
 28: optional string sessionExitRate7d,
 29: optional double score_30d,
 30: optional string nextToken,
}

struct ExtraClientInfo{
 1: optional string lastInteractiveId,
 2: optional string lastInteractiveTimestamp,
 3: optional string lastInteractiveType,
}

struct Location{
 1: string country,
 2: optional string state,
 3: optional string city,
 4: optional double coordinateX,
 5: optional double coordinateY,
 6: optional list<string> field,
}

struct Request{
 1: string interfaceName,
 2: UserInfo userInfo,
 3: string platformId,
 4: string tabId,
 5: i32 num,
 6: byte type,
 7: optional string logId,
 8: optional string resourceId,
 9: optional string resourceType,
 10: optional list<string> languageList,
 11: optional list<string> appSourceList,
 12: optional string nextToken,
 13: optional string timeZone,
 14: optional string clientVersion,
 15: optional string isDebugModeOpen,
 16: optional string timeSign,
 17: optional string execTimeSign,
 18: optional string execTimeDelay,
 19: string originalInterfaceName,
 20: optional string lastRefreshTime,
 21: optional ExtraClientInfo extraClientInfo,
 22: optional Location location,
 23: optional list<string> blockPublisherList,
 24: optional list<string> realTimeClickVideoList,
 25: optional string requestFromApp,
 26: optional bool isRetryRequest,
 27: optional bool isRobotRequest,
 28: optional list<string> oldLanguageList,
 29: optional list<string> interestTagList,
}

struct Banner {
 1: string bannerId,
}

struct Card {
 1: string cardId,
 2: optional list<Result> resultList,
 3: optional InternalUse internalUse,
}

struct PublisherInfo {
 1: string id,
 2: optional string reason,
}

struct StickerGroup {
 1: string id,
 2: string name,
 3: optional i32 status,
 4: optional i32 order,
 5: optional list<string> stickerIds,
 6: optional i64 updateTime,
 7: optional i64 createTime,
 8: optional string originalIconUrl,
}

struct Sticker {
 1: string id,
 2: string stickerName,
 3: optional i32 status,
 4: optional string stickerType,
 5: optional string stickerGroup,
 6: optional list<string> countries,
 7: optional i64 updateTime,
 8: optional i64 createTime,
 9: optional string originalPackageUrl,
 10: optional string stickerThumbnailUrl,
 11: optional string originalStickerUrl,
}

struct Result{
 1: string resultType,
 2: optional i64 recallSign,
 3: ShortVideo shortVideo,
 4: string debugInfo,
 5: string attachContent,
 6: InternalUse internalUse,
 7: optional PublisherInfo publisherInfo,
 8: optional string id,
 9: LiveStream liveStream,
}

struct ShortVideo{
 1: string id,
 2: string type,
 3: string name,
 4: i64 viewCount,
 5: i64 likeCount,
 6: i64 wShareCount,
 7: i64 downCount,
 8: string contentUrl,
 9: string description,
 10: ThumbnailInfo thumbnailInfo,
 11: string downLoadUrl,
 12: optional double distance,
}

struct LiveStream{
 1: string streamId,
 2: optional string publisherId,
}

struct Badge{
 1: i32 maxDays,
 2: i32 maxWeeks,
 3: i32 totalDays,
 4: i64 timestamp,
}

struct Response{
 1: list<Result> resultList,
 2: optional string logId,
 3: string nextToken,
 4: InternalUse internalUse,
 5: optional i32 resultNum,
 6: optional bool redDot,
 7: list<Banner> bannerList,
 8: list<Card> cardList,
 9: optional i32 status,
10:list<string> publisherIds,
11: optional bool needRetry,
12: optional string logMap,
13: optional list<StickerGroup> stickerGroupList,
14: optional list<Sticker> stickerList,
15: optional Badge badge,
}

struct InternalRequest{
 1: string interfaceName,
 2: optional list<string> resourceIdList,
 3: optional string additionalInfo,
}

struct InternalResponse{
 1: list<InternalResult> internalResultList,
 2: string errorMessage,
 3: optional bool processingStatus,
}

struct InternalResult{
 1: string publisherId,
 2: i32 num,
 3: optional list<string> videoIdList,
 4: optional InternalShortVideo internalShortVideo,
 5: optional string filterInfo,
}

struct InternalShortVideo{
 1: string id,
 2: optional string likeInfo,
 3: optional string featStat30d,
 4: optional string bigHead,
 5: optional string mlTags,
 6: optional string duration,
 7: optional string isDuplicated,
 8: optional bool isUgcContent,
 9: optional string featStat0d,
 10: optional i32 isDelogo,
}

service RecommendService {
    string test(1:string word),
    Response recommend(1:Request req),
    Response fetchBanner(1:Request req),
    Response fetchTabs(1:Request req),
    Response fetchStatus(1:Request req),
    InternalResponse internalRecommend(1:InternalRequest internalReq),
}
