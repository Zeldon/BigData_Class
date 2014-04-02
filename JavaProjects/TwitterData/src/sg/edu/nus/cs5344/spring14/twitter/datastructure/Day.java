package sg.edu.nus.cs5344.spring14.twitter.datastructure;

import org.apache.hadoop.io.VIntWritable;

/**
 * A class indicating on specific day
 * @author Tobias Bertelsen
 *
 */
public class Day extends VIntWritable {


	/**
	 * Day zero of the day index. <b>DO NOT MODIFY</b>
	 * 
	 * <p>
	 * It is specified to be 1359562320000L which is 2014-12-31.
	 * Run this code, to calculate new values:
	 * <pre>
	 * System.out.println((new SimpleDateFormat("yyyy-mm-dd")).parse("2013-12-31").getTime());
	 * </pre>
	 */
	private static final Time DAY_ZERO = new Time(1359562320000L);

	public Day(){
	}
	public Day(Time time) {
		super(time.getDaysBetween(DAY_ZERO));
	}
}
