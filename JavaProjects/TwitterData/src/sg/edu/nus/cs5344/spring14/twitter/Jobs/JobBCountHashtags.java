package sg.edu.nus.cs5344.spring14.twitter.Jobs;

import static java.lang.Math.min;
import static sg.edu.nus.cs5344.spring14.twitter.ConfUtils.getFirstDayPath;
import static sg.edu.nus.cs5344.spring14.twitter.TwConsts.MIN_DAYLY_TWEETS;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import sg.edu.nus.cs5344.spring14.twitter.datastructure.Day;
import sg.edu.nus.cs5344.spring14.twitter.datastructure.Hashtag;
import sg.edu.nus.cs5344.spring14.twitter.datastructure.Tweet;
import sg.edu.nus.cs5344.spring14.twitter.datastructure.collections.DayHashtagPair;

/**
 * This job counts how many times a hashtag is used in one specific day.
 *
 * Further it detects the first day of the first hashtag, and stores it in HDFS.
 * This is done EXTREMELY hacky, by merging in into the other hashtag/day
 * calculation, but it saves an entire extra run of the data, to extract just
 * one value.
 *
 * @author tobber
 *
 */
public class JobBCountHashtags {

	// Unique marker key for detecting the start day in the data
	private static final DayHashtagPair START_DATE_MARKER = new DayHashtagPair(new Day(Integer.MAX_VALUE), new Hashtag(
			"START##DATE"));

	public static class MapperImpl extends Mapper<Tweet, NullWritable, DayHashtagPair, VIntWritable> {
		private int minDay = Integer.MAX_VALUE;

		private VIntWritable ONE = new VIntWritable(1);

		@Override
		protected void map(Tweet tweet, NullWritable nothing, Context context) throws IOException, InterruptedException {
			Day day = tweet.getTime().getDay();
			for (Hashtag hashtag : tweet.getHashTagList()) {
				context.write(new DayHashtagPair(day, hashtag), ONE);
			}

			// Maintain the first day we see.
			minDay = min(minDay, day.get());
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// Output the first day in this mapper,
			context.write(START_DATE_MARKER, new VIntWritable(minDay));
		}
	}

	public static class ReducerImpl extends Reducer<DayHashtagPair, VIntWritable, DayHashtagPair, VIntWritable> {
		@Override
		protected void reduce(DayHashtagPair pair, Iterable<VIntWritable> counts, Context context) throws IOException,
				InterruptedException {
			if (START_DATE_MARKER.equals(pair)) {
				saveStartDay(counts, context);
				return;
			}

			int sum = 0;
			for (VIntWritable count : counts) {
				sum += count.get();
			}

			if (sum > MIN_DAYLY_TWEETS) {
				context.write(pair, new VIntWritable(sum));
			}
		}

		private void saveStartDay(Iterable<VIntWritable> candidates, Context context) {
			// Find min day
			int minDay = Integer.MAX_VALUE;
			for (VIntWritable writable : candidates) {
				int candidate = writable.get();
				minDay = min(minDay, candidate);
			}
			if (minDay == Integer.MAX_VALUE) {
				throw new RuntimeException("Could not find the first day");
			}

			// Save it
			OutputStream fileStream = null;
			try {
				try {
					Configuration conf = context.getConfiguration();
					FileSystem fs = FileSystem.get(conf);
					fileStream = fs.create(getFirstDayPath(conf));
					PrintStream writer = new PrintStream(fileStream);
					writer.print(minDay);
				} finally {
					if (fileStream != null) {
						fileStream.close();
					}
				}
			} catch (IOException e) {
				throw new RuntimeException("Can't save the first day, abort: ", e);
			}
		}
	}

}
