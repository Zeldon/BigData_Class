package sg.edu.nus.cs5344.spring14.twitter.Jobs;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import sg.edu.nus.cs5344.spring14.twitter.datastructure.Hashtag;
import sg.edu.nus.cs5344.spring14.twitter.datastructure.collections.DayCountPair;
import sg.edu.nus.cs5344.spring14.twitter.datastructure.collections.DayHashtagPair;

public class JobCFindTrends {

	public static class MapperImpl extends Mapper<DayHashtagPair, VIntWritable, Hashtag, DayCountPair> {
		@Override
		protected void map(DayHashtagPair dayHashPair, VIntWritable count, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			throw new UnsupportedOperationException("Not Yet Implemented!");
		}
	}


	public static class ReducerImpl extends Reducer<Hashtag, DayCountPair, Hashtag, NullWritable> {
		@Override
		protected void reduce(Hashtag hashtag, Iterable<DayCountPair> counts, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			throw new UnsupportedOperationException("Not Yet Implemented!");
		}
	}

}
