package EECS6893.bitcoin;


import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class BitcoinReducer extends Reducer <Text, Text, Text, Text>{
	 
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		int num = 0;
		long sum = 0;
		for (Text t: values) {
			num += 1;
			String[] info = t.toString().split(",");
			String amntS = info[info.length - 1];
			double amnt = Double.parseDouble(amntS.substring(1, amntS.length()));
			sum += amnt;
			
		}
		context.write(new Text(key), 
				new Text(String.valueOf(num) + String.format("\t\t%.2f", (double)sum / num)));
		
	}

}
