package sg.edu.nus.cs5344.spring14.twitter.Jobs;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import sg.edu.nus.cs5344.spring14.twitter.datastructure.Tweet;
import sg.edu.nus.cs5344.spring14.twitter.datastructure.collections.DayHashtagPair;

public class JobBCountHashtags {

	public static class MapperImpl extends Mapper<Tweet, NullWritable, DayHashtagPair, VIntWritable> {
		@Override
		protected void map(Tweet tweet, NullWritable nothing, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			throw new UnsupportedOperationException("Not Yet Implemented!");
		}
	}


	public static class ReducerImpl extends Reducer<DayHashtagPair, VIntWritable, DayHashtagPair, VIntWritable> {
		@Override
		protected void reduce(DayHashtagPair pair, Iterable<VIntWritable> counts, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			throw new UnsupportedOperationException("Not Yet Implemented!");
		}
	}

}
