package EECS6893.bitcoin;


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


public class BitCoinRunner extends Configured implements Tool {
	public int run(String args[]) throws Exception {
		String inputDir = "input/Bitcoin/bitcoin_data.txt";
		String outputDir1 = "output/bitcoin";
		Job job = Job.getInstance();
		job.setJobName("bitcoin");
		job.setJarByClass(BitCoinRunner.class);
		
		job.setMapperClass(BitcoinMapper.class);
		job.setReducerClass(BitcoinReducer.class);
	
		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.addInputPath(job, new Path(inputDir));
		//job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 1);
		FileOutputFormat.setOutputPath(job, new Path(outputDir1));			
		job.setNumReduceTasks(1);	
	
		job.setMapOutputKeyClass(Text.class);				
		job.setMapOutputValueClass(Text.class);					
		job.setOutputKeyClass(Text.class);					
		job.setOutputValueClass(Text.class);				
					
		return job.waitForCompletion(true) ? 0 : -1;
		
	}
	public static void main(String args[]) throws Exception {
		BitCoinRunner BitcoinDriver = new BitCoinRunner();
		int res = ToolRunner.run(BitcoinDriver, args);
		System.exit(res);
		
	}
}
