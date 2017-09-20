package EECS6893.airline;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class AirlineAnalysisRunner extends Configured implements Tool {
	public int run(String args[]) throws Exception {
		String inputDir = "input/airline/2008.csv";
		String outputDir1 = "output/airline/day_of_week";
		//String teamFight = "input/teamfights.csv";
		Job job = Job.getInstance();
		job.setJobName("test");
		job.setJarByClass(AirlineAnalysisRunner.class);
		//job.addCacheFile(new Path(teamFight).toUri());
		
		job.setMapperClass(AirlineMapper.class);
		job.setReducerClass(AirlineReducer.class);
	
		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.addInputPath(job, new Path(inputDir));
		//job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 1);
		FileOutputFormat.setOutputPath(job, new Path(outputDir1));			
		job.setNumReduceTasks(1);	
	
		job.setMapOutputKeyClass(IntWritable.class);				
		job.setMapOutputValueClass(IntWritable.class);					
		job.setOutputKeyClass(IntWritable.class);					
		job.setOutputValueClass(IntWritable.class);				
					
		return job.waitForCompletion(true) ? 0 : -1;
		
	}
	public static void main(String args[]) throws Exception {
		AirlineAnalysisRunner Dota2Driver = new AirlineAnalysisRunner();
		int res = ToolRunner.run(Dota2Driver, args);
		System.exit(res);
		
	}
}
