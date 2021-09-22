package mx.j2.recommend.recall.impl;

import mx.j2.recommend.util.MXJudgeUtils;

import java.util.*;
import java.util.stream.Collectors;

/**
 * 精选大 V 抽取概率提高 3 倍
 * <p>
 * 方法：让精选大 V 重复出现 3 次，再 shuffle，再去重
 */
@SuppressWarnings("unused")
public class RealTimePublisherRecallAllChosenV extends RealTimePublisherRecallAll {

    @Override
    protected void shufflePublisher(List<String> pubIds) {
        // 精选大 V 数量变为 3 倍
        List<String> repeatIdList = new ArrayList<>();

        for (String id : pubIds) {
            if (CHOSEN_V_SET.contains(id)) {
                repeatIdList.add(id);
                repeatIdList.add(id);
            }
        }

        // 如果没有精选大 V，使用父类版本，并及时返回止损
        if (MXJudgeUtils.isEmpty(repeatIdList)) {
            super.shufflePublisher(pubIds);
            return;
        }

        // 重复 id 加入到原数组里
        pubIds.addAll(repeatIdList);

        // 打散
        Collections.shuffle(pubIds);

        // 去重
        List<String> distinctList = pubIds.stream().distinct().collect(Collectors.toList());

        // 写回原数组
        pubIds.clear();
        pubIds.addAll(distinctList);
    }

    /**
     * 精选大 V 数组
     */
    private static String[] CHOSEN_V_ARRAY = {
            "14919833046109",
            "152467260703571041",
            "153432308660194580",
            "14918810596505",
            "16001423.fa8206db5ef345619952a25b1ff370c3.1851",
            "15350335779701818",
            "15745458936207776",
            "152741175986112454",
            "15725802817984897",
            "15348329556158803",
            "153033016460080909",
            "153227576463947809",
            "152633056343575102",
            "16000471.a7f4995020ab420fbae8e9785fb5affc.1125",
            "14919084800874",
            "153269094853150083",
            "151400194706836210",
            "151235535393450143",
            "151000849440367569",
            "153175015265913179",
            "14919818708023",
            "151922881534508475",
            "151839028606239184",
            "14917977819837",
            "14919653167264",
            "152639929906335826",
            "14919999856693",
            "153129224730498879",
            "12115579803885366886444",
            "151755574587929712",
            "15776383142899710",
            "12115976978730915295316",
            "12103634589182597586410",
            "14917974167200",
            "14919892954965",
            "14919999069114",
            "14918810439661",
            "15952250061903305",
            "12113639762199613713185",
            "14917976539107",
            "12115084268632280320587",
            "14919643968191",
            "14919636044445",
            "14916377462127",
            "16000176.7d8410be35ad47de978f9c7b6eab2760.0726",
            "151698705376951968",
            "152735826643360566",
            "153540372702680246",
            "1510207964251021377",
            "12103996947583128158857",
            "151832669930208872",
            "14918826892580",
            "153164448946982140",
            "14918130867291",
            "14918290920001",
            "14918210765552",
            "14919167630499",
            "14919953786169",
            "14918059513454",
            "14918077377100",
            "15903022790193312",
            "151386208251579298",
            "15933656817109285",
            "14918708333092",
            "12101205635176687677307",
            "14917718931746",
            "14919414844542",
            "14919557998604",
            "15299413291172413",
            "15167903564824898",
            "153175746962490738",
            "152722138988021577",
            "14919304637224"
    };

    /**
     * 精选大 V 集合
     */
    private static Set<String> CHOSEN_V_SET;

    /*
     * 精选大 V 集合初始化
     */
    static {
        CHOSEN_V_SET = new HashSet<>(Arrays.asList(CHOSEN_V_ARRAY));
    }

    public static void main(String[] args) {
        List<String> pubId = new ArrayList<>();
        pubId.add("v1");
        pubId.add("v2");
        pubId.add("v3");
        pubId.add("p4");
        pubId.add("p5");
        pubId.add("p6");
        pubId.add("p7");
        pubId.add("p8");
        pubId.add("p9");
        pubId.add("p10");

        RealTimePublisherRecallAll recall = new RealTimePublisherRecallAll();
        List<String> pubId1 = new ArrayList<>(pubId);
        recall.shufflePublisher(pubId1);
        System.out.println(pubId1);

        List<String> pubId2 = new ArrayList<>(pubId);
        RealTimePublisherRecallAllChosenV recall2 = new RealTimePublisherRecallAllChosenV();
        recall2.shufflePublisher(pubId2);
        System.out.println(pubId2);
    }
}
