package sg.edu.nus.cs5344.spring14.twitter.Jobs;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import sg.edu.nus.cs5344.spring14.twitter.datastructure.Tweet;


public class JobAParseText {

	private static final NullWritable NULL = NullWritable.get();

	public static class MapperImpl extends Mapper<LongWritable, Text, Tweet, NullWritable> {
		private String lineBuffer = null;

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();

			// Handle line breaks escaped by backslash
			if (lineBuffer != null) {
				// This line is a continuation of the previous
				line = lineBuffer + line;
				lineBuffer = null;
			}
			if (line.charAt(line.length() -1) == '\\') {
				// This line continues on the next
				lineBuffer = line;
				return;
			}

			// Parse the line and output the tweet.
			try {
				context.write(new Tweet(line), NULL);
			} catch (IllegalArgumentException e) {
				System.out.println("Could not parse tweet: " + line);
				System.out.println("Reason: " + e.getMessage());
			}

		}
	}


	public static class ReducerImpl extends Reducer<Tweet, NullWritable, Tweet, NullWritable> {
		@Override
		protected void reduce(Tweet tweet, Iterable<NullWritable> nothing, Context context)
				throws IOException, InterruptedException {
			context.write(tweet, NULL);
		}
	}

}
