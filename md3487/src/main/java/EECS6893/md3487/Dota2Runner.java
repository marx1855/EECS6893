package EECS6893.md3487;
import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Dota2Runner {
	public int run(String args[]) throws Exception {
		String inputDir = "input";
		String outputDir1 = "Teamfights";
		String teamFight = "input/teamfights.csv";
		Job job = Job.getInstance();
		job.setJobName("test");
		job.setJarByClass(Dota2Runner.class);
		job.addCacheFile(new Path(teamFight).toUri());
		
		job.setMapperClass(TeamFightMapper.class);
		job.setReducerClass(TeamFightReducer.class);
	
		job.setInputFormatClass(NLineInputFormat.class);
		NLineInputFormat.addInputPath(job, new Path(inputDir));
		job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 1);
		FileOutputFormat.setOutputPath(job, new Path(outputDir1));			
		job.setNumReduceTasks(1);	
	
		job.setMapOutputKeyClass(IntWritable.class);				
		job.setMapOutputValueClass(Text.class);					
		job.setOutputKeyClass(IntWritable.class);					
		job.setOutputValueClass(Text.class);				
					
		return job.waitForCompletion(true) ? 0 : -1;
		
	}
	public static void main(String args[]) {
		
	}
}
