package EECS6893.bitcoin;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BitcoinMapper extends Mapper <LongWritable, Text, Text, Text> {
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] bitcoinTrans = value.toString().split(",");
		//int index = 0;
		/*if (!bitcoinTrans[0].equals("index"))
			index = Integer.parseInt(bitcoinTrans[0]);*/
		String genderInfo = bitcoinTrans[5] + "->" +bitcoinTrans[6];
		if (!bitcoinTrans[5].equals("gender_from"))
			context.write(new Text(genderInfo), value);	
	}

}

