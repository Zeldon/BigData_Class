package sg.edu.nus.cs5344.spring14.twitter;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import sg.edu.nus.cs5344.spring14.twitter.Jobs.JobAParseText;
import sg.edu.nus.cs5344.spring14.twitter.datastructure.Tweet;

public class Main {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO (tbertelsen) Do everything :)
		// Parse command line
		final Configuration conf = new Configuration();
		final String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();


		Job jobA = createJobA(conf, new Path("SampleData100.txt"), new Path("temp/output/"));

		jobA.waitForCompletion(true);
	}

	private static Job createJobA(final Configuration conf, final Path input, final Path output) throws IOException {
		Job job = new Job(conf, "Parse Tweets");
		job.setJarByClass(JobAParseText.MapperImpl.class);
		// Map
		FileInputFormat.addInputPath(job, input);
		// job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setMapperClass(JobAParseText.MapperImpl.class);
		job.setMapOutputKeyClass(Tweet.class);
		job.setMapOutputValueClass(NullWritable.class);
		// Reduce
		job.setReducerClass(JobAParseText.ReducerImpl.class);
		job.setNumReduceTasks(4);
		job.setOutputKeyClass(Tweet.class);
		job.setOutputValueClass(NullWritable.class);
		// Store intermediate data in as sequence file (binary) format
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileOutputFormat.setOutputPath(job, output);
		return job;
	}




}
