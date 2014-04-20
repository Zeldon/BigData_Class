package sg.edu.nus.cs5344.spring14.twitter;

public class TwConsts {

	/**
	 * Defines the number of previous days we compare the current day against
	 * when finding trends.
	 */
	public static final int TREND_LOOKBACK = 7;

	/**
	 * How many times more the day of the week should count,
	 * compared to other days.
	 *
	 * Zero means no change
	 *
	 * A value of 3 mean that the day 7 days ago, will count as 4 days (3 days more)
	 */
	public static final double TREND_WEEKDAY_BOOST = 3.0;

	/**
	 * The number of trends to keep
	 */
	public static final int NUM_BEST_TRENDS = 50;

	/**
	 * All day indexes strictly lower than this value, should not be included.
	 * Value is 158 = 2012-09-06, which is the first day after the great spikes
	 * in data frequency.
	 */
	public static final int FILTER_FIRST_DAY = 158;

	public static final int FILTER_MIN_HASHTAGS_PER_DAY = 110000;

	public static final String DAY_STATS_DATA_FOLDER_ATT = "DAY_STATS_FOLDER";
}
