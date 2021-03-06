package EECS6893.airline;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AirlineMapper extends Mapper <LongWritable, Text, IntWritable, Text> {
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] airlines = value.toString().split(",");
		int dayOfWeek = Integer.parseInt(airlines[3]);
		context.write(new IntWritable(dayOfWeek), value);	
	}

}

