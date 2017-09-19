package EECS6893.md3487;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TeamFightMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] teamFight = value.toString().split(",");
		int matchId = Integer.parseInt(teamFight[0]);
		String matchInfo = teamFight[1] + "," + teamFight[2] + "," + teamFight[3] + "," + teamFight[4]; 
		context.write(new IntWritable(matchId), new Text(matchInfo));
	}

}
