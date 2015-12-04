package pagerank;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import pagerank.MatrixVectorMult.FirstMap;
import pagerank.MatrixVectorMult.FirstReduce;

public class RemoveDeadends {

	enum myCounters{ 
		NUMNODES;
	}

	static class Map extends Mapper<LongWritable, Text, Text, Text> {
		
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
			{
			// TO DO
			StringTokenizer itr = new StringTokenizer(value.toString());
			String k = itr.nextToken();
			String v = itr.nextToken();
			context.write(new Text(k), new Text("1\t"+v));
			context.write(new Text(v), new Text("0\t"+k));			
			}
		}
	

	static class Reduce extends Reducer<Text, Text, Text, Text> {
		
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			//TO DO
			boolean Dead = true;
			ArrayList<Text> sortie = new ArrayList<Text>();
			for(Text value:values){
				StringTokenizer itr = new StringTokenizer(value.toString());
				int temp = Integer.parseInt(itr.nextToken());
				Text text = new Text(itr.nextToken());
				if(temp == 1){
					Dead = false;
				}
				
				else{
					sortie.add(text);
				}
				
			}
			if(!Dead){
				for(int i=0;i<sortie.size();i++){
					context.write(sortie.get(i),key);
				
				}
				Counter c = context.getCounter(myCounters.NUMNODES);
				c.increment(1);	
			}
			if(Dead == false){
				
			}
		}
}

	public static void job(Configuration conf) throws IOException, ClassNotFoundException, InterruptedException{
		
		
		boolean existDeadends = true;
		
		/* You don't need to use or create other folders besides the two listed below.
		 * In the beginning, the initial graph is copied in the processedGraph. After this, the working directories are processedGraphPath and intermediaryResultPath.
		 * The final output should be in processedGraphPath. 
		 */
		
		FileUtils.copyDirectory(new File(conf.get("graphPath")), new File(conf.get("processedGraphPath")));
		String intermediaryDir = conf.get("intermediaryResultPath");
		String currentInput = conf.get("processedGraphPath");
		
		long nNodes = conf.getLong("numNodes", 0);
		System.out.println("*******                "+nNodes);
		long nn = 0;
		
		while(existDeadends)
		{
			Job job = Job.getInstance(conf);
			job.setJobName("deadends job");
			/* TO DO : configure job and move in the best manner the output for each iteration
			 * you have to update the number of nodes in the graph after each iteration,
			 * use conf.setLong("numNodes", nNodes);
			*/
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);

			job.setMapperClass(Map.class);
			job.setReducerClass(Reduce.class);

			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			
			FileInputFormat.setInputPaths(job, new Path(conf.get("processedGraphPath")));
			FileOutputFormat.setOutputPath(job, new Path(conf.get("intermediaryResultPath")));
			
			job.waitForCompletion(true);
			
			FileUtils.deleteDirectory(new File(conf.get("processedGraphPath")));
			FileUtils.copyDirectory(new File(conf.get("intermediaryResultPath")), new File(conf.get("processedGraphPath")));
			FileUtils.deleteDirectory(new File(conf.get("intermediaryResultPath")));
			
			
			Counters counters = job.getCounters();
			Counter c = counters.findCounter(myCounters.NUMNODES);
			//System.out.println("*******                "+c.getValue());

			
			//System.out.println(nNodes);
			
			if(c.getValue()==nn){
				existDeadends = false;	
				conf.setLong("numNodes", c.getValue());
			}
			else{
				nn = c.getValue();
			}
			//if(nNodes==8)existDeadends = false;
			
		}	
//			Job job = Job.getInstance(conf);
//			job.setJobName("deadends job");
//			job.setMapOutputKeyClass(Text.class);
//			job.setMapOutputValueClass(Text.class);
//	
//			job.setMapperClass(Map.class);
//			job.setReducerClass(Reduce.class);
//	
//			job.setInputFormatClass(TextInputFormat.class);
//			job.setOutputFormatClass(TextOutputFormat.class);
//			
//			FileInputFormat.setInputPaths(job, new Path(intermediaryDir));
//			FileOutputFormat.setOutputPath(job, new Path(currentInput));
//			
//			job.waitForCompletion(true);
//			Counters counters = job.getCounters();
//			Counter c = counters.findCounter(myCounters.NUMNODES);
		// when you finished implementing delete this line
		//throw new UnsupportedOperationException("Implementation missing");
		
		
	}
	
}