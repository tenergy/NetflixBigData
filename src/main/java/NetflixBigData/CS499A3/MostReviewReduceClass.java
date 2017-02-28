package NetflixBigData.CS499A3;


import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class MostReviewReduceClass extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

	private HashMap<IntWritable, IntWritable> mostreviews = new HashMap<IntWritable, IntWritable>();

	@Override
	protected void reduce(IntWritable userid, Iterable<IntWritable> values,
			Reducer<IntWritable, IntWritable, IntWritable, IntWritable>.Context context) throws IOException, InterruptedException {

		int sum = 0;
		Iterator<IntWritable> i = values.iterator();
		while(i.hasNext()) {
			int count = i.next().get();
			sum += count;
		}
		
		mostreviews.put(new IntWritable(userid.get()), new IntWritable(sum));
		
		if (mostreviews.size() > 10){
			IntWritable min = getMinKey(mostreviews);
			mostreviews.remove(min);
		}
	}
	
	private IntWritable getMinKey(HashMap<IntWritable, IntWritable> map) {
	    IntWritable minKey = null;
	    int minValue = Integer.MAX_VALUE;   
	    for(IntWritable key : map.keySet()) {
	        IntWritable value = map.get(key);
	        if(value.get() < minValue) {
	            minValue = value.get();
	            minKey = key;
	        }
	    }
	    return minKey;
	}	
	
	@Override 
	protected void cleanup(Context context) throws IOException, InterruptedException{
		
		for(IntWritable userid : mostreviews.keySet()){
			context.write(userid, mostreviews.get(userid));
		}
	}

	
}