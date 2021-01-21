package funlibs.date;

import java.util.TimeZone;

public class DateUtil {
	public static final long EPOCH_TIME_20000101 = 946684800000L;
	public static final long ONE_MILLI_SECOND = 1L;
	public static final long ONE_SECOND = ONE_MILLI_SECOND * 1000L;
	public static final long ONE_MINUTE = ONE_SECOND * 60L;
	public static final long ONE_HOUR = ONE_MINUTE * 60L;
	public static final long ONE_DAY = ONE_HOUR * 24L;

	private static final long OFFSET = TimeZone.getDefault().getRawOffset();

	public static long ymd() {
		return ymd(System.currentTimeMillis());
	}

	public static long ymd(long timestamp) {
		long hmsS = (timestamp + OFFSET) % ONE_DAY;
		return timestamp - hmsS;
	}

	public static int dayFlomEpoch(long timestamp) {
		long hmsS = (timestamp + OFFSET) % ONE_DAY;
		return (int) ((timestamp - 946684800000L - hmsS) / ONE_DAY);
	}

	private DateUtil() {
	}
}
