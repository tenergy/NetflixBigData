package NetflixBigData.CS499A3;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Driver extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Driver(), args);
		System.exit(exitCode);
	}

	public int run(String[] args) throws Exception {
		if (args.length != 1) {
			System.err.printf("Usage: %s needs two arguments, input and output files\n", getClass().getSimpleName());
			return -1;
		}
		
		int returnvalue = -1;

		Job highestavgrating = new Job();
		highestavgrating.setJarByClass(Driver.class);
		highestavgrating.setJobName("HighestAvgRating");

		FileInputFormat.addInputPath(highestavgrating, new Path(args[0]));
		FileOutputFormat.setOutputPath(highestavgrating, new Path("./highestavgrating"));

		highestavgrating.setOutputKeyClass(IntWritable.class);
		highestavgrating.setOutputValueClass(DoubleWritable.class);
		highestavgrating.setOutputFormatClass(TextOutputFormat.class);

		highestavgrating.setMapperClass(HighestAvgRatingMapClass.class);
		highestavgrating.setReducerClass(HighestAvgRatingReduceClass.class);


		int returnValue1 = highestavgrating.waitForCompletion(true) ? 0:1;

		if (highestavgrating.isSuccessful()) {
			System.out.println("Job was successful");
		} else if(!highestavgrating.isSuccessful()) {
			System.out.println("Job was not successful");
		}
		
		Job mostreview = new Job();
		mostreview.setJarByClass(Driver.class);
		mostreview.setJobName("MostReview");

		FileInputFormat.addInputPath(mostreview, new Path(args[0]));
		FileOutputFormat.setOutputPath(mostreview, new Path("./mostreviews"));

		mostreview.setOutputKeyClass(IntWritable.class);
		mostreview.setOutputValueClass(IntWritable.class);
		mostreview.setOutputFormatClass(TextOutputFormat.class);

		mostreview.setMapperClass(MostReviewsMapClass.class);
		mostreview.setReducerClass(MostReviewReduceClass.class);

		int returnValue2 = mostreview.waitForCompletion(true) ? 0:1;

		if (mostreview.isSuccessful()) {
			System.out.println("Job was successful");
		} else if(!mostreview.isSuccessful()) {
			System.out.println("Job was not successful");
		}
		
		
		if((returnValue1 == 0) && (returnValue2 == 0)){
			returnvalue = 0;
		}
		else{
			returnvalue = 1;
		}
		
		return returnvalue;
	}
	
	

}

