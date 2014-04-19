package sg.edu.nus.cs5344.spring14.twitter;

import static sg.edu.nus.cs5344.spring14.twitter.TwConsts.FIRST_DAY_FILE_ATT;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class ConfUtils {

	public static Path getFirstDayPath(Configuration conf) {
		String file = conf.get(FIRST_DAY_FILE_ATT);
		if (file == null) {
			throw new RuntimeException(FIRST_DAY_FILE_ATT + " was not set in the configuration");
		}
		return new Path(file);
	}

}
