package funlibs.logging;

public class SimpleLogging {
	public static SimpleLogging of() {
		Throwable t = new Throwable();
		StackTraceElement[] stackTrace = t.getStackTrace();
		SimpleLogging simpleLogging = new SimpleLogging();
		if (stackTrace.length > 1) {
			simpleLogging.loggingGroup = new String[] { stackTrace[1].getClassName() };
		}
		return simpleLogging;
	}

	public static SimpleLogging forClassDebug() {
		return of() //
			.showDateTime() //
			.showShortLogName() //
			.showThreadName() //
			.dateTimeFormat("yyyy-MM-dd HH:mm:ss:SSS") //
			.loggingGroupLogLevel("DEBUG") //
			.initialize() //
		;
	}

	public static SimpleLogging forPackageDebug() {
		Throwable t = new Throwable();
		StackTraceElement[] stackTrace = t.getStackTrace();
		SimpleLogging simpleLogging = new SimpleLogging();
		if (stackTrace.length > 1) {
			try {
				simpleLogging.loggingGroup = new String[] { Class.forName(stackTrace[1].getClassName()).getPackage().getName() };
			} catch (ClassNotFoundException e) {
				simpleLogging.loggingGroup = new String[] { stackTrace[1].getClassName() };
			}
		}
		return simpleLogging //
			.showDateTime() //
			.showShortLogName() //
			.showThreadName() //
			.dateTimeFormat("yyyy-MM-dd HH:mm:ss:SSS") //
			.loggingGroupLogLevel("DEBUG") //
			.initialize() //
		;
	}

	public static SimpleLogging forAllDebug() {
		return of() //
			.showDateTime() //
			.showShortLogName() //
			.showThreadName() //
			.dateTimeFormat("yyyy-MM-dd HH:mm:ss:SSS") //
			.defalutLogLevel("DEBUG") //
			.initialize() //
		;
	}

	private SimpleLogging() {
	}

	private boolean showDateTime = false;
	private String dateTimeFormat = null;
	private boolean showThreadName = false;
	private boolean showShortLogName = false;
	private String[] loggingGroup = null;
	private String loggingGroupLogLevel = null;
	private String defalutLogLevel = null;
	private boolean showLogName = true;
	private boolean levelInBrackets = false;

	public SimpleLogging showDateTime() {
		this.showDateTime = true;
		return this;
	}

	public SimpleLogging dateTimeFormat(String dateTimeFormat) {
		this.dateTimeFormat = dateTimeFormat;
		return this;
	}

	public SimpleLogging showThreadName() {
		this.showThreadName = true;
		return this;
	}

	public SimpleLogging showShortLogName() {
		this.showShortLogName = true;
		return this;
	}

	public SimpleLogging loggingGroup(String... loggingGroup) {
		this.loggingGroup = loggingGroup;
		return this;
	}

	public SimpleLogging loggingGroupLogLevel(String loggingGroupLogLevel) {
		this.loggingGroupLogLevel = loggingGroupLogLevel;
		return this;
	}

	public SimpleLogging defalutLogLevel(String defalutLogLevel) {
		this.defalutLogLevel = defalutLogLevel;
		return this;
	}

	public SimpleLogging notShowLogName() {
		this.showLogName = false;
		return this;
	}

	public SimpleLogging levelInBrackets() {
		this.levelInBrackets = true;
		return this;
	}

	public SimpleLogging initialize() {
		if (showDateTime) {
			System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
		}
		if (dateTimeFormat != null) {
			System.setProperty("org.slf4j.simpleLogger.dateTimeFormat", dateTimeFormat);
		}
		if (showThreadName) {
			System.setProperty("org.slf4j.simpleLogger.showThreadName", "true");
		}
		if (showShortLogName) {
			System.setProperty("org.slf4j.simpleLogger.showShortLogName", "true");
		}
		if (!showLogName) {
			System.setProperty("org.slf4j.simpleLogger.showLogName", "false");
		}
		if (levelInBrackets) {
			System.setProperty("org.slf4j.simpleLogger.levelInBrackets", "true");
		}
		if (loggingGroupLogLevel != null && loggingGroup != null) {
			for (String g : loggingGroup) {
				System.setProperty("org.slf4j.simpleLogger.log." + g, loggingGroupLogLevel);
			}
		} else if (defalutLogLevel != null) {
			System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", defalutLogLevel);
		} else {
			throw new RuntimeException("log level is required.");
		}
		return this;
	}
}
