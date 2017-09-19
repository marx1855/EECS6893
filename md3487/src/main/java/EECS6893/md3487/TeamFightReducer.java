package EECS6893.md3487;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TeamFightReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
	public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		int teamFightNum = 0;
		int sumFightTime = 0;
		int sumDeathNum = 0;
		
		for (Text s: values) {
			String[] matchInfo = s.toString().split(",");
			int startTime = Integer.parseInt(matchInfo[0]);
			int endTime = Integer.parseInt(matchInfo[1]);
			int deathNum = Integer.parseInt(matchInfo[3]);
			
			teamFightNum ++;
			sumFightTime += (endTime - startTime);
			sumDeathNum += deathNum;
		}
		
		String avgFightTime = String.valueOf((double) (sumFightTime / teamFightNum));
		String avgDeathNum = String.valueOf((double) (sumDeathNum / teamFightNum));
		
		String output = teamFightNum + "," + avgFightTime + "," + avgDeathNum + "," + sumDeathNum;
		context.write(key,  new Text(output));
	}

}
