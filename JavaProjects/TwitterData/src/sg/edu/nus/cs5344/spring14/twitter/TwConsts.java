package sg.edu.nus.cs5344.spring14.twitter;

public class TwConsts {


	// Based on http://www2.lv.psu.edu/jxm57/irp/chisquar.html
	// 95% significance = 3.84
	// 99% significance = 6.64
	public static final double CHI_THRESHOLD = 6.64;

	public static final int MIN_DAYLY_TWEETS = 10;

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


	public static final String DAY_STATS_DATA_FOLDER_ATT = "DAY_STATS_FOLDER";
}
