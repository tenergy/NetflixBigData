package NetflixBigData.CS499A3;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MostReviewsMapClass extends Mapper<LongWritable, Text, IntWritable, IntWritable>{

	private static final IntWritable one = new IntWritable(1);
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, IntWritable>.Context context) throws IOException, InterruptedException{
				
		String line = value.toString();
		StringTokenizer st = new StringTokenizer(line, ",");
		st.nextToken();
		int userid = Integer.parseInt(st.nextToken());

		context.write(new IntWritable(userid), one);
	}
}
