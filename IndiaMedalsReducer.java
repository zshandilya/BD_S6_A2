package BDS6A2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class IndiaMedalsReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable>{
	
	IntWritable tMedals;
	
	public void setup(Context context) {
		
		tMedals = new IntWritable();
	}
	
	public void reduce(IntWritable key, Iterable<IntWritable> medals, Context context) throws IOException, InterruptedException {
		
		int sum=0;
		for(IntWritable medal:medals)
			sum=sum+medal.get();
		
		tMedals.set(sum);
		context.write(key, tMedals);
	}

}
