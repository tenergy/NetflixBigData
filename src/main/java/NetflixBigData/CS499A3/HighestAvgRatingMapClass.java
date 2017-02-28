package NetflixBigData.CS499A3;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HighestAvgRatingMapClass extends Mapper<LongWritable, Text, IntWritable, DoubleWritable> {

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, DoubleWritable>.Context context) throws IOException, InterruptedException{
		
		String line = value.toString();
		StringTokenizer st = new StringTokenizer(line, ",");
		int movie = Integer.parseInt(st.nextToken());
		st.nextToken();
		double rating = Double.parseDouble(st.nextToken());

		context.write(new IntWritable(movie), new DoubleWritable(rating));
	}
}




