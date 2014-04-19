package sg.edu.nus.cs5344.spring14.twitter;

import static sg.edu.nus.cs5344.spring14.twitter.FileLocations.getInput;
import static sg.edu.nus.cs5344.spring14.twitter.FileLocations.getOutputForJob;
import static sg.edu.nus.cs5344.spring14.twitter.FileLocations.getSpecaialFolder;
import static sg.edu.nus.cs5344.spring14.twitter.FileLocations.getSpecialFile;
import static sg.edu.nus.cs5344.spring14.twitter.FileLocations.getTextOutputPath;
import static sg.edu.nus.cs5344.spring14.twitter.TwConsts.FIRST_DAY_FILE_ATT;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import sg.edu.nus.cs5344.spring14.twitter.Jobs.JobAParseText;
import sg.edu.nus.cs5344.spring14.twitter.Jobs.JobBCountHashtags;
import sg.edu.nus.cs5344.spring14.twitter.Jobs.JobCFindTrends;
import sg.edu.nus.cs5344.spring14.twitter.Jobs.JobDDayStats;
import sg.edu.nus.cs5344.spring14.twitter.datastructure.Day;
import sg.edu.nus.cs5344.spring14.twitter.datastructure.Hashtag;
import sg.edu.nus.cs5344.spring14.twitter.datastructure.Trend;
import sg.edu.nus.cs5344.spring14.twitter.datastructure.Tweet;
import sg.edu.nus.cs5344.spring14.twitter.datastructure.collections.DayCountPair;
import sg.edu.nus.cs5344.spring14.twitter.datastructure.collections.DayHashtagPair;

public class TwMain {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// Parse command line
		final Configuration conf = new Configuration();
		final String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		CmdArguments cmdArgs = CmdArguments.instantiate(otherArgs);

		prepOutDir(conf, getSpecaialFolder());
		conf.set(FIRST_DAY_FILE_ATT, getSpecialFile("firstDay").toString());


		List<Job> jobs = new ArrayList<Job>();

		if (!cmdArgs.skipJob("A")) {
			jobs.add(createJobA(conf, getInput(), getOutputForJob("A")));
		}
		if (!cmdArgs.skipJob("B")) {
			jobs.add(createJobB(conf, getOutputForJob("A"), getOutputForJob("B")));
		}
		if (!cmdArgs.skipJob("C")) {
			jobs.add(createJobC(conf, getOutputForJob("B"), getOutputForJob("C")));
		}
		if (!cmdArgs.skipJob("D")) {
			jobs.add(createJobD(conf, getOutputForJob("A"), getTextOutputPath("DayStats")));
		}


		for (Job job : jobs) {

			System.out.println("\nRunning Job:" + job.getJobName());
			if(!job.waitForCompletion(true)) {
				throw new RuntimeException("Job failed: " + job.getJobName());
			}
		}


		// Write human readable files
		FileSystem fs = FileSystem.get(conf);
		OutputStream fileStream = null;
		try {
			fileStream = fs.create(FileLocations.getTextOutputPath("trends.txt"));
			printOutput(conf, getOutputForJob("C"), new Hashtag(), new Trend(), new PrintStream(fileStream), System.out);
		} finally {
			if (fileStream != null) {
				fileStream.close();
			}
		}
	}

	private static Job createJobA(final Configuration conf, final Path input, final Path output) throws IOException {
		prepOutDir(conf, output);

		Job job = new Job(conf, "Parse Tweets");
		job.setJarByClass(JobAParseText.class);
		// Map
		FileInputFormat.addInputPath(job, input);
		// job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setMapperClass(JobAParseText.MapperImpl.class);
		job.setMapOutputKeyClass(Tweet.class);
		job.setMapOutputValueClass(NullWritable.class);
		// Reduce
		job.setReducerClass(JobAParseText.ReducerImpl.class);
		job.setOutputKeyClass(Tweet.class);
		job.setOutputValueClass(NullWritable.class);
		// Store intermediate data in as sequence file (binary) format
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileOutputFormat.setOutputPath(job, output);
		return job;
	}

	private static Job createJobB(final Configuration conf, final Path input, final Path output) throws IOException {
		prepOutDir(conf, output);

		Job job = new Job(conf, "Count Hashtags");
		job.setJarByClass(JobBCountHashtags.class);
		// Map
		FileInputFormat.addInputPath(job, input);
		// job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setMapperClass(JobBCountHashtags.MapperImpl.class);
		job.setMapOutputKeyClass(DayHashtagPair.class);
		job.setMapOutputValueClass(VIntWritable.class);
		// Reduce
		job.setReducerClass(JobBCountHashtags.ReducerImpl.class);
		job.setOutputKeyClass(DayHashtagPair.class);
		job.setOutputValueClass(VIntWritable.class);
		// Store intermediate data in as sequence file (binary) format
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileOutputFormat.setOutputPath(job, output);
		return job;
	}

	private static Job createJobC(final Configuration conf, final Path input, final Path output) throws IOException {
		prepOutDir(conf, output);

		Job job = new Job(conf, "Find Trends");
		job.setJarByClass(JobCFindTrends.MapperImpl.class);
		// Map
		FileInputFormat.addInputPath(job, input);
		// job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setMapperClass(JobCFindTrends.MapperImpl.class);
		job.setMapOutputKeyClass(Hashtag.class);
		job.setMapOutputValueClass(DayCountPair.class);
		// Reduce
		job.setReducerClass(JobCFindTrends.ReducerImpl.class);
		job.setOutputKeyClass(Hashtag.class);
		job.setOutputValueClass(Trend.class);
		// Store intermediate data in as sequence file (binary) format
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileOutputFormat.setOutputPath(job, output);
		return job;
	}

	private static Job createJobD(final Configuration conf, final Path input, final Path output) throws IOException {
		prepOutDir(conf, output);

		Job job = new Job(conf, "Day Stats");
		job.setJarByClass(JobDDayStats.class);
		// Map
		FileInputFormat.addInputPath(job, input);
		// job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setMapperClass(JobDDayStats.MapperImpl.class);
		job.setMapOutputKeyClass(Day.class);
		job.setMapOutputValueClass(VIntWritable.class);
		// Reduce
		job.setReducerClass(JobDDayStats.ReducerImpl.class);
		job.setOutputKeyClass(Day.class);
		job.setOutputValueClass(VIntWritable.class);
		// Store intermediate data in as sequence file (binary) format
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, output);
		return job;
	}

	/**
	 * Prints small files, for which it would be a waste to run an entire job.
	 *
	 * @param conf
	 * @param folder
	 * @param key
	 * @param value
	 * @param outs
	 * @throws IOException
	 */
	private static <K extends Writable, V extends Writable> void printOutput(Configuration conf, Path folder, K key,
			V value, PrintStream... outs) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] listFies = fs.globStatus(new Path(folder, "part-*"));
		if (listFies.length == 0) {
			for (PrintStream out : outs) {
				out.println("NO OUTPUT FILE FOUND IN " + folder);
			}
		}

		for (FileStatus fileStatus : listFies) {
			for (PrintStream out : outs) {
				out.println(fileStatus.getPath());
			}
			Reader reader = null;
			try {
				reader = new SequenceFile.Reader(fs, fileStatus.getPath(), conf);
				while (reader.next(key, value)) {
					for (PrintStream out : outs) {
						out.println(key + "\t" + value);
					}
				}
			} finally {
				if (reader != null) {
					reader.close();
				}
			}
		}
	}

	private static void prepOutDir(final Configuration conf, final Path dir) throws IOException {
		final FileSystem fs = FileSystem.get(conf);
		fs.delete(dir, true);
		fs.mkdirs(dir.getParent());
	}

}
