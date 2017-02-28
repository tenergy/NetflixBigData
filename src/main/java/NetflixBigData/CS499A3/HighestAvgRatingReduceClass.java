package NetflixBigData.CS499A3;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class HighestAvgRatingReduceClass extends Reducer<IntWritable, DoubleWritable, Text, DoubleWritable>{

	private HashMap<IntWritable, DoubleWritable> countMap = new HashMap<IntWritable, DoubleWritable>();
	private HashMap<Integer, String> movietitles = new HashMap<Integer, String>();
	
	@Override
	public void setup(Context context) throws IOException, InterruptedException{
		
		File file = new File("/home/movie_titles.txt");

		try{
			
			BufferedReader reader = new BufferedReader(new FileReader(file));
			String entry;
			StringTokenizer st;
			int movieid;
			String movietitle;
			
			while((entry = reader.readLine()) != null){
				st = new StringTokenizer(entry, ",");
				movieid = Integer.parseInt(st.nextToken());
				st.nextToken();
				movietitle = st.nextToken();
				movietitles.put(movieid, movietitle);
			}
			reader.close();
		}catch (FileNotFoundException e){
			e.printStackTrace();
		}catch (IOException e){
			e.printStackTrace();
		}
	}
	
	@Override
	protected void reduce(IntWritable key, Iterable<DoubleWritable> values, Reducer<IntWritable, DoubleWritable, Text, DoubleWritable>.Context context) throws IOException, InterruptedException{
		
		double sum = 0.0;
		double count = 0.0;
		Iterator<DoubleWritable> i = values.iterator();
		
		while(i.hasNext()){
			sum += i.next().get();
			count++;
		}
		
		countMap.put(new IntWritable(key.get()),  new DoubleWritable(sum/count));
		
		if(countMap.size() > 10){
			IntWritable lowestrating = getMinKey(countMap);
			countMap.remove(lowestrating);
		}
	}
	
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException{
		
		for(IntWritable movie : countMap.keySet()){
			Text movietitle = new Text(movietitles.get(movie.get()));
			double formatted = Math.round(countMap.get(movie).get()*100.0)/100.0;
			context.write(movietitle, new DoubleWritable(formatted));
		}
		
		
	}
	
	private IntWritable getMinKey(HashMap<IntWritable, DoubleWritable> map) {
	    IntWritable minKey = null;
	    double minValue = Double.MAX_VALUE;   
	    for(IntWritable key : map.keySet()) {
	        DoubleWritable value = map.get(key);
	        if(value.get() < minValue) {
	            minValue = value.get();
	            minKey = key;
	        }
	    }
	    return minKey;
	}	
}
