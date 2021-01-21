package funlibs.date;

import static org.junit.Assert.assertTrue;

import java.util.Date;

import org.junit.Test;

public class DateUtilTest {

	@Test
	public void test() {
		long millis = System.currentTimeMillis();
		long ymd = DateUtil.ymd(millis);
		int dayFromEpoch = DateUtil.dayFlomEpoch(millis);
		int dayFromEpoch2 = DateUtil.dayFlomEpoch(ymd);
		assertTrue("milli second must be 0", ymd % DateUtil.ONE_MILLI_SECOND == 0L);
		assertTrue("second must be 0", ymd % DateUtil.ONE_SECOND == 0L);
		assertTrue("minute must be 0", ymd % DateUtil.ONE_MINUTE == 0L);
		assertTrue("hour must be 0", ymd % DateUtil.ONE_HOUR == 0L);
		System.out.println(String.format("millis:%d, DateUtil.ymd(millis):%d, millis:%s, ymd(millis):%s", millis, DateUtil.ymd(millis), new Date(millis).toString(), new Date(DateUtil.ymd(millis)).toString()));
		System.out.println(String.format("dayFromEpoch:%d, dayFromEpoch(ymd):%d", dayFromEpoch, dayFromEpoch2));
	}

}
