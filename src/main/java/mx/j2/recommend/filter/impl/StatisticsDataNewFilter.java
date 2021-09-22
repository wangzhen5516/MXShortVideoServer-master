package mx.j2.recommend.filter.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_model.statistics_document.BaseStatisticsDocument;
import mx.j2.recommend.data_source.StatisticDataSource;
import mx.j2.recommend.manager.DataSourceManager;
import mx.j2.recommend.statistic_conf.StatisticConf;
import mx.j2.recommend.util.BaseMagicValueEnum;

import java.lang.reflect.Field;
import java.util.Map;

/**
 * @author ：zhongrenli
 * @date ：Created in 3:19 下午 2021/4/19
 */
public class StatisticsDataNewFilter extends BaseFilter<BaseDataCollection>{


    @Override
    public boolean isFilted(BaseDocument doc, BaseDataCollection dc) {
        if (doc == null) {
            return true;
        }

        if (!doc.statisticsDocument.isLoadSuccess()) {
            return false;
        }

        if (!(doc.recallName.contains("PoolRecall") && BaseMagicValueEnum.HIGH_LEVEL.equals(doc.poolLevel))
                && !doc.recallName.contains("StrategyTagPoolRecallWeightedShuffleNumber2")
                && !doc.recallName.contains("TrendingRealtimeV10")) {
            return false;
        }

        StatisticDataSource dataSource = DataSourceManager.INSTANCE.getStatisticDataSource();
        Map<String, StatisticConf> map = dataSource.getStatisticConfMap();

        for (Map.Entry<String, StatisticConf> entry : map.entrySet()) {
            if (!doc.statisticsDocument.exist(entry.getKey())) {
                continue;
            }

            StatisticConf conf = entry.getValue();
            if (null == conf) {
                continue;
            }

            if (null != conf.getExclude() && !conf.getExclude().isEmpty()) {
                if (conf.getExclude().contains(dc.recommendFlow.name)) {
                    return false;
                }
            }

            BaseStatisticsDocument baseStatisticsDocument = doc.statisticsDocument.get(entry.getKey());
            if (null != conf.getPreConditionGreaterThan() && !conf.getPreConditionGreaterThan().isEmpty()) {
                Boolean v = checkPre(conf.getPreConditionGreaterThan(), dataSource, baseStatisticsDocument, ">");
                if (null != v) {
                    coverProperties(doc, baseStatisticsDocument);
                    return v;
                }
            }

            if (null != conf.getPreConditionLessThan() && !conf.getPreConditionLessThan().isEmpty()) {
                Boolean v = checkPre(conf.getPreConditionLessThan(), dataSource, baseStatisticsDocument, "<");
                if (null != v) {
                    coverProperties(doc, baseStatisticsDocument);
                    return v;
                }
            }

            boolean filter = false;
            if (null != conf.getBaseConditionGreaterThan() && !conf.getBaseConditionGreaterThan().isEmpty()) {
                // 小流量配置
                if (null != conf.getSmallFlowConf() && !conf.getSmallFlowConf().isEmpty() && conf.getSmallFlowConf().containsKey(dc.recommendFlow.name)) {
                    StatisticConf tempConf = conf.getSmallFlowConf().get(dc.recommendFlow.name);
                    filter = checkBase(tempConf.getBaseConditionGreaterThan(), dataSource, baseStatisticsDocument, ">");
                } else {
                    filter = checkBase(conf.getBaseConditionGreaterThan(), dataSource, baseStatisticsDocument, ">");
                }
            }

            if (filter) {
                return true;
            }

            if (null != conf.getBaseConditionLessThan() && !conf.getBaseConditionLessThan().isEmpty()) {
                filter = checkBase(conf.getBaseConditionLessThan(), dataSource, baseStatisticsDocument, "<");
            }
            coverProperties(doc, baseStatisticsDocument);
            return filter;
        }
        return false;
    }

    private Boolean checkPre(Map<String, Double> map, StatisticDataSource dataSource, BaseStatisticsDocument doc, String then) {
        for (Map.Entry<String, Double> e : map.entrySet()) {
            String fieldName = dataSource.getFieldName(e.getKey());
            if (null == fieldName) {
                continue;
            }

            try {
                Field field = BaseStatisticsDocument.class.getDeclaredField(fieldName);
                field.setAccessible(true);

                if (">".equals(then)) {
                    if (field.getDouble(doc) < e.getValue()) {
                        return false;
                    }
                } else if ("<".equals(then)) {
                    if (field.getDouble(doc) > e.getValue()) {
                        return false;
                    }
                }

            } catch (NoSuchFieldException | IllegalAccessException noSuchFieldException) {
                noSuchFieldException.printStackTrace();
                return false;
            }
        }
        return null;
    }

    private boolean checkBase(Map<String, Double> map, StatisticDataSource dataSource, BaseStatisticsDocument doc, String then) {
        for (Map.Entry<String, Double> e : map.entrySet()) {
            String fieldName = dataSource.getFieldName(e.getKey());
            if (null == fieldName) {
                continue;
            }

            try {
                Field field = BaseStatisticsDocument.class.getDeclaredField(fieldName);
                field.setAccessible(true);

                if (">".equals(then)) {
                    if (field.getDouble(doc) < e.getValue()) {
                        return true;
                    }
                } else if ("<".equals(then)) {
                    if (field.getDouble(doc) > e.getValue()) {
                        return true;
                    }
                }

            } catch (NoSuchFieldException | IllegalAccessException noSuchFieldException) {
                noSuchFieldException.printStackTrace();
                return false;
            }
        }
        return false;
    }

    private void coverProperties(BaseDocument bdoc, BaseStatisticsDocument doc) {
        try {
            bdoc.statisticsDocument.setShareRate30d(doc.getShareRate());
            bdoc.statisticsDocument.setDownloadRate30d(doc.getDownloadRate());
            bdoc.statisticsDocument.setLikeRate30d(doc.getLikeRate());
            bdoc.statisticsDocument.setFinishedRate30d(doc.getFinishRate());
            bdoc.statisticsDocument.setViewAll30d(doc.getViewAll());
            bdoc.statisticsDocument.setPlayRate30d(doc.getPlayRate());
            bdoc.statisticsDocument.setFinishRetentionSum10s30d(doc.getFinishRetentionSum10s());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
