package mx.j2.recommend.data_source;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @Author: xiaoling.zhu
 * @Date: 2020-11-09
 */

public class DateTimeDataSource extends BaseDataSource {
    private long currentTime;

    public DateTimeDataSource() {
        this.currentTime = System.currentTimeMillis();
    }

    public void scheduleUpdateTime() {
        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
        scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                currentTime = System.currentTimeMillis();
//                System.out.println(currentTime);
            }
        }, 1, 1, TimeUnit.MINUTES);
    }

    public enum TimeChoose {
        SNACK_MORTNING("06:00", "10:00"),
        DEFAULT("00:00", "13:25");

        private static final String INDIAN_ZONE_ID = "Asia/Kolkata";
        private long beginTime;
        private long endTime;

        TimeChoose(String begin, String end) {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm");
            LocalTime beginLocalTime = LocalTime.parse(begin, formatter);
            LocalDateTime beginLocalDateTime = beginLocalTime.atDate(LocalDate.now());
            beginTime = beginLocalDateTime.atZone(ZoneId.of(INDIAN_ZONE_ID)).toInstant().toEpochMilli();
            LocalTime endLocalTime = LocalTime.parse(end, formatter);
            LocalDateTime endLocalDateTime = endLocalTime.atDate(LocalDate.now());
            endTime = endLocalDateTime.atZone(ZoneId.of(INDIAN_ZONE_ID)).toInstant().toEpochMilli();
        }
    }

    public boolean isInTimeInterVal(TimeChoose chooser) {
        if (chooser.beginTime < this.currentTime && chooser.endTime > this.currentTime) {
            return true;
        } else {
            return false;
        }
    }
}
