package sg.edu.nus.cs5344.spring14.twitter.Jobs;

import static sg.edu.nus.cs5344.spring14.twitter.TwConsts.CHI_THRESHOLD;
import static sg.edu.nus.cs5344.spring14.twitter.TwConsts.MIN_DAYLY_TWEETS;
import static sg.edu.nus.cs5344.spring14.twitter.TwConsts.NUM_BEST_TRENDS;
import static sg.edu.nus.cs5344.spring14.twitter.TwConsts.TREND_LOOKBACK;
import static sg.edu.nus.cs5344.spring14.twitter.TwConsts.TREND_WEEKDAY_BOOST;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Queue;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import sg.edu.nus.cs5344.spring14.twitter.ConfUtils;
import sg.edu.nus.cs5344.spring14.twitter.datastructure.Hashtag;
import sg.edu.nus.cs5344.spring14.twitter.datastructure.Trend;
import sg.edu.nus.cs5344.spring14.twitter.datastructure.Trend.TrendsBuilder;
import sg.edu.nus.cs5344.spring14.twitter.datastructure.collections.DayCountPair;
import sg.edu.nus.cs5344.spring14.twitter.datastructure.collections.DayHashtagPair;
import sg.edu.nus.cs5344.spring14.twitter.datastructure.collections.TopKList;

public class JobCFindTrends {

	public static class MapperImpl extends Mapper<DayHashtagPair, VIntWritable, Hashtag, DayCountPair> {
		@Override
		protected void map(DayHashtagPair dayHashPair, VIntWritable count, Context context) throws IOException,
				InterruptedException {
			context.write(dayHashPair.getHashtag(), new DayCountPair(dayHashPair.getDay(), count));
		}
	}

	public static class ReducerImpl extends Reducer<Hashtag, DayCountPair, Hashtag, Trend> {

		TopKList<Trend> topKTrends = new TopKList<Trend>(NUM_BEST_TRENDS);

		private int firstDay;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			Configuration conf = context.getConfiguration();
			FileSystem fs = FileSystem.get(conf);
			Scanner sc = new Scanner(fs.open(ConfUtils.getFirstDayPath(conf)));
			firstDay = sc.nextInt();
			sc.close();
		}

		@Override
		protected void reduce(Hashtag hashtag, Iterable<DayCountPair> counts, Context context) throws IOException,
				InterruptedException {

			Deque<DayCountPair> prevDays = new ArrayDeque<DayCountPair>();

			Trend bestTrend = null;
			TrendsBuilder builder = null;

			for (DayCountPair pair : counts) {
				int thisDay = pair.getDay().get();

				// Skip first days to avoid classify popular as trending.
				// We add 1 for the current day, and 1 because the first day is likely half
				if (thisDay <= firstDay + TREND_LOOKBACK + 2) {
					prevDays.add(pair.copy());
					continue;
				}

				// Remove days that are too old
				while (prevDays.size() > 0 &&
						prevDays.peek().getDay().get() < thisDay - TREND_LOOKBACK) {
					prevDays.poll();
				}

				// Calculate chi squared chi^2 = (e-o)^2/e
				// See http://blogs.ischool.berkeley.edu/i290-abdt-s12/
				// Lecture 6 slide 10-13
				double expected = calcExpected(prevDays, thisDay);
				double observed = pair.getCount().get();
				double chiSq = ((observed - expected) * (observed - expected)) / expected;

				// Detect trends, build Trend objects, and keep the best
				if (chiSq > CHI_THRESHOLD) {
					if (builder == null) {
						// First day of trend
						builder = new TrendsBuilder(hashtag);
					} else if (prevDays.isEmpty() ||
							thisDay - prevDays.peekLast().getDay().get() != 1) {
						// Handle quiet days between trends
						bestTrend = updateBestTrend(bestTrend, builder);
						builder = new TrendsBuilder(hashtag);
					}
					builder.withDay(pair, chiSq);
				} else if (builder != null) {
					// First day not trending
					bestTrend = updateBestTrend(bestTrend, builder);
					builder = null;
				}
			}

			if (bestTrend != null) {
				topKTrends.add(bestTrend);
			}
		}

		private Trend updateBestTrend(Trend bestTrend, TrendsBuilder builder) {
			Trend candidateTrend = builder.build();
			if (bestTrend == null || bestTrend.compareTo(candidateTrend) < 0) {
				bestTrend = candidateTrend;
			}
			return bestTrend;
		}

		private double calcExpected(Queue<DayCountPair> prevDays, int thisDay) {
			double sum = 0.0;
			double numDays = 0.0;
			for (DayCountPair prevPair : prevDays) {
				boolean isSameWeekday = (prevPair.getDay().get() - thisDay) % 7 == 0;
				double factor = isSameWeekday ? TREND_WEEKDAY_BOOST : 1.0;
				sum += prevPair.getCount().get() * factor;
				numDays += factor;
			}
			int missingDays = TREND_LOOKBACK - prevDays.size();
			sum += MIN_DAYLY_TWEETS * missingDays;
			numDays += missingDays;
			return sum/numDays;
		}

		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			for (Trend trend : topKTrends.sortedList()) {
				context.write(trend.getHashTag(), trend);
			}
		}
	}
}
