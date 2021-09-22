package mx.j2.recommend.recall.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_source.ElasticCacheSource;
import mx.j2.recommend.data_source.LocalCacheDataSource;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXJudgeUtils;
import mx.j2.recommend.util.MXStringUtils;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public class VideosOfTheTagRecall extends SearchEngineRecall<BaseDataCollection> {

    private static final String INDEX_URL = DefineTool.CategoryEnum.SHORT_VIDEO.getIndexAndType();
    private static final String SENSITIVE_WORD_REDIS_KEY = "sensitive_word_list";
    private static final String SENSITIVE_WORD_LOCAL_CACHE_KEY = "sensitive_word_list";

    private final Set<String> iplHashTag = new HashSet<>(
            Arrays.asList("kheltakatak",
                    "fandancemove",
                    "batbalance",
                    "bolcricket",
                    "chakhdephatte",
                    "laphaokkr",
                    "turoarmacha",
                    "mipaltandance",
                    "hallabol",
                    "goorangearmy"
            )
    );

    // TODO 投资方运营tag，豁免音乐过滤
    private final String PEPSI_TAG = "";

    @Override
    public boolean skip(BaseDataCollection dc) {
        if (MXJudgeUtils.isEmpty(dc.req.resourceType) || MXJudgeUtils.isEmpty(dc.req.resourceId)
                || !"tag".equals(dc.req.resourceType)) {
            return true;
        }
        return false;
    }

    @Override
    public void constructRequestURL(BaseDataCollection dc) {
        if (isBadDescription(dc)) {
            return;
        }

        getEsData(dc);
    }

    private void getEsData(BaseDataCollection dc) {
        String esContent;
        String HEAD_ES_CONTENT_FORMAT;
        String NEXT_ES_CONTENT_FORMAT;
        String lowerTag = dc.req.resourceId.toLowerCase();
        if (iplHashTag.contains(lowerTag)) {
            HEAD_ES_CONTENT_FORMAT = "{\"from\":0,\"size\":%s,\"query\":{\"bool\":{\"must_not\":[{\"nested\":{\"path\":\"hashtag_match_info_es\",\"query\":{\"bool\":{\"filter\":{\"range\":{\"hashtag_match_info_es.score\":{\"lte\":6}}},\"must\":[{\"match\":{\"hashtag_match_info_es.hash\":\"%s\"}}]}}}}],\"must\":[{\"match\":{\"desc_tag\":\"%s\"}},{\"match\":{\"status\":\"1\"}},{\"exists\":{\"field\":\"is_porn\"}},{\"match\":{\"human_reviewed_status\":\"1\"}}]}},\"sort\":[{\"hashtag_heat\":{\"order\":\"desc\",\"missing\":0}},{\"online_time\":{\"order\":\"desc\"}}]}";
            NEXT_ES_CONTENT_FORMAT = "{\"from\":0,\"size\":%s,\"query\":{\"bool\":{\"must_not\":[{\"nested\":{\"path\":\"hashtag_match_info_es\",\"query\":{\"bool\":{\"filter\":{\"range\":{\"hashtag_match_info_es.score\":{\"lte\":6}}},\"must\":[{\"match\":{\"hashtag_match_info_es.hash\":\"%s\"}}]}}}}],\"must\":[{\"match\":{\"desc_tag\":\"%s\"}},{\"match\":{\"status\":\"1\"}},{\"exists\":{\"field\":\"is_porn\"}},{\"match\":{\"human_reviewed_status\":\"1\"}}]}},\"search_after\":[\"%s\",\"%s\"],\"sort\":[{\"hashtag_heat\":{\"order\":\"desc\",\"missing\":0}},{\"online_time\":{\"order\":\"desc\"}}]}";
        } else if (PEPSI_TAG.equals(lowerTag)) {
            HEAD_ES_CONTENT_FORMAT = "{\"from\":0,\"size\":%s,\"query\":{\"bool\":{\"must\":[{\"match\":{\"desc_tag\":\"%s\"}},{\"match\":{\"status\":\"1\"}},{\"exists\":{\"field\":\"is_porn\"}}]}},\"sort\":[{\"hashtag_heat\":{\"order\":\"desc\",\"missing\":0}},{\"online_time\":{\"order\":\"desc\"}}]}";
            NEXT_ES_CONTENT_FORMAT = "{\"from\":0,\"size\":%s,\"query\":{\"bool\":{\"must\":[{\"match\":{\"desc_tag\":\"%s\"}},{\"match\":{\"status\":\"1\"}},{\"exists\":{\"field\":\"is_porn\"}}]}},\"search_after\":[\"%s\",\"%s\"],\"sort\":[{\"hashtag_heat\":{\"order\":\"desc\",\"missing\":0}},{\"online_time\":{\"order\":\"desc\"}}]}";
        } else {
            HEAD_ES_CONTENT_FORMAT = "{\"from\":0,\"size\":%s,\"query\":{\"bool\":{\"must_not\":[{\"nested\":{\"path\":\"hashtag_match_info_es\",\"query\":{\"bool\":{\"filter\":{\"range\":{\"hashtag_match_info_es.score\":{\"lte\":6}}},\"must\":[{\"match\":{\"hashtag_match_info_es.hash\":\"%s\"}}]}}}}],\"must\":[{\"match\":{\"desc_tag\":\"%s\"}},{\"match\":{\"status\":\"1\"}},{\"exists\":{\"field\":\"is_porn\"}}]}},\"sort\":[{\"hashtag_heat\":{\"order\":\"desc\",\"missing\":0}},{\"online_time\":{\"order\":\"desc\"}}]}\n";
            NEXT_ES_CONTENT_FORMAT = "{\"from\":0,\"size\":%s,\"query\":{\"bool\":{\"must_not\":[{\"nested\":{\"path\":\"hashtag_match_info_es\",\"query\":{\"bool\":{\"filter\":{\"range\":{\"hashtag_match_info_es.score\":{\"lte\":6}}},\"must\":[{\"match\":{\"hashtag_match_info_es.hash\":\"%s\"}}]}}}}],\"must\":[{\"match\":{\"desc_tag\":\"%s\"}},{\"match\":{\"status\":\"1\"}},{\"exists\":{\"field\":\"is_porn\"}}]}},\"search_after\":[\"%s\",\"%s\"],\"sort\":[{\"hashtag_heat\":{\"order\":\"desc\",\"missing\":0}},{\"online_time\":{\"order\":\"desc\"}}]}\n";
        }

        if (MXStringUtils.isNotEmpty(dc.req.nextToken) && dc.req.nextToken.split("\\|").length == 2) {
            String[] tokens = dc.req.nextToken.split("\\|");
            int size = dc.req.num == 0 ? 40 : dc.req.num * 3;
            if (PEPSI_TAG.equals(lowerTag)) {
                esContent = String.format(NEXT_ES_CONTENT_FORMAT, size, lowerTag, tokens[0], tokens[1]);
            } else {
                esContent = String.format(NEXT_ES_CONTENT_FORMAT, size, lowerTag, lowerTag, tokens[0], tokens[1]);
            }
        } else {
            int size = dc.req.num == 0 ? 40 : dc.req.num * 3;
            if (PEPSI_TAG.equals(lowerTag)) {
                esContent = String.format(HEAD_ES_CONTENT_FORMAT, size, lowerTag);
            } else {
                esContent = String.format(HEAD_ES_CONTENT_FORMAT, size, lowerTag, lowerTag);
            }
        }

        String elasticSearchRequest = String.format(requestUrlFormat, INDEX_URL);

        dc.addToESRequestList(
                elasticSearchRequest,
                esContent,
                this.getName(), "",
                DefineTool.EsType.VIDEO.getTypeName()
        );
        dc.resultFromMap.put(this.getName(), DefineTool.RecallFrom.ES.getName());
        dc.searchEngineRecallerSet.add(this.getName());
    }

    private boolean isBadDescription(BaseDataCollection dc) {
        LocalCacheDataSource localCacheDataSource = MXDataSource.cache();
        Set<String> sensitiveWords = localCacheDataSource.getSensitiveWordCache(SENSITIVE_WORD_LOCAL_CACHE_KEY);
        if (MXJudgeUtils.isEmpty(sensitiveWords)) {
            ElasticCacheSource elasticCacheSource = MXDataSource.redis();
            sensitiveWords = elasticCacheSource.getSensitiveWordsList(SENSITIVE_WORD_REDIS_KEY);
            if (MXJudgeUtils.isEmpty(sensitiveWords)) {
                return false;
            }
            sensitiveWords = sensitiveWords.stream().peek(word -> word.replace(" ", "").replace(".", "").replace("+", "")).collect(Collectors.toSet());
            localCacheDataSource.setSensitiveWordCache(SENSITIVE_WORD_LOCAL_CACHE_KEY, sensitiveWords);
        }
        return sensitiveWords.contains(dc.req.resourceId);
    }
}
