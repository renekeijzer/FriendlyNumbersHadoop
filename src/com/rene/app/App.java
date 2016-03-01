package com.rene.app;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;



/**
 * Hello world!
 *
 */
public class App
{

	
  public static void main(String[] args) throws Exception {
    Job job = Job.getInstance();
    job.setJarByClass(App.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.setMapperClass(FriendlyNumberMapper.class);
    job.setReducerClass(FriendlyNumberReducer.class);
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    job.waitForCompletion(true);
  }
}


class FriendlyNumberMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	public void map(LongWritable Key, Text value, Context context) throws IOException, InterruptedException {
		String[] tokens = value.toString().split("\\s");
		for (String s : tokens) {
			int amice = this.sumFactors(Integer.parseInt(s));
			if(amice > 10){
				context.write(new Text(s), new IntWritable(amice));
			}
		} 
	}
	
  private static int sumFactors(int n)
  {
      int sum = 0;
      for (int div=1; div<=n/2; div++)
      {
          if (n%div == 0)
          {
              sum += div;
          }
      }
      return sum;
  }


}


class FriendlyNumberReducer extends Reducer<Text, IntWritable, IntWritable, IntWritable> {
	HashMap<Integer, Integer> results = new HashMap<Integer, Integer>();
	
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {		
			for(IntWritable value : values){
				if(results.containsKey(Integer.parseInt(value.toString()))){					
					
					Tuple<Integer, Integer> inversedPair = 	new Tuple<Integer, Integer>(
							Integer.parseInt(value.toString()),
							Integer.parseInt(key.toString())
					);
					
					if(results.get(inversedPair.x).equals(inversedPair.y)){
						context.write(
								new IntWritable(inversedPair.x),
								new IntWritable(inversedPair.y)
								);
					}
				
				}else{
					results.put(Integer.parseInt(key.toString()), Integer.parseInt(value.toString()));
				}
				
			}	
	}	
}
