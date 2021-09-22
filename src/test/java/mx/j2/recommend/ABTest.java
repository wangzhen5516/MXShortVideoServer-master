package mx.j2.recommend;

import junit.framework.TestCase;
import org.apache.commons.lang3.RandomStringUtils;

import java.util.Random;

/**
 * Created by zhangxuejian on 2018/9/11.
 */
public class ABTest extends TestCase {
    private static final int SMALL_FLOWNAME_BASE_NUMBER = 10000;
    private static final int EXP_TIME = 10;

    private static final String[] userIds = {
            "e17d6eb3-822b-4d2b-bb77-7cb0532e6442592506275"
    };

    public void test() {
//        for (String userId : userIds) {
//            int r = Math.abs(userId.hashCode() + 1) % SMALL_FLOWNAME_BASE_NUMBER;
//            System.out.println(String.format("%s -> %s", userId, r));
//        }

        String id = "ff88170e-939c-4aa1-b1b8-e0d75e03c4b393328656";
        System.out.println(calculateRange(id));

        int code = Math.abs(id.hashCode() + 1) % 256;
        System.out.println(code);

        String id2 = "14917985012696";
        System.out.println(calculateHash(id2) % 16);


        for (int i = 0; i < 1000; i++) {
            String userId= RandomStringUtils.randomAlphanumeric(32);
            int r = calculateRange(userId);
            if (5000 < r && r < 5499) {
                System.out.println(userId);
                break;
            }
        }
    }

    private int calculateRange(String userId) {
        return Math.abs(userId.hashCode() + 1) % SMALL_FLOWNAME_BASE_NUMBER;
    }

    public static String getRandomString2(int length){
        Random random=new Random();
        StringBuffer sb=new StringBuffer();
        for(int i=0;i<length;i++){
            int number=random.nextInt(3);
            long result=0;
            switch(number){
                case 0:
                    result=Math.round(Math.random()*25+65);
                    sb.append(String.valueOf((char)result));
                    break;
                case 1:
                    result=Math.round(Math.random()*25+97);
                    sb.append(String.valueOf((char)result));
                    break;
                case 2:
                    sb.append(String.valueOf(new Random().nextInt(10)));
                    break;
            }


        }
        return sb.toString();
    }

    private int calculateHash(String str) {
        return str.hashCode() & Integer.MAX_VALUE;
    }
}
