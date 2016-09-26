package BDS6A2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class IndiaMedalsMapper extends Mapper<LongWritable,Text,IntWritable,IntWritable>{
	
	IntWritable year;
	IntWritable medals;
	String[] line;
	
	public void setup(Context context) {
		
		year = new IntWritable();
		medals = new IntWritable();
	}
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		line = value.toString().split("\t");
		
		if(line[2].matches("India")) {
			
			year.set(Integer.parseInt(line[3]));
			medals.set(Integer.parseInt(line[9]));
			context.write(year,medals);
		}
		
	}

}
