//retail Store (C1)
import java.io.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.io.LongWritable.DecreasingComparator;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class TopLossPid 
{
	public static class MyMapper1 extends Mapper<LongWritable,Text,Text,Text>
	{
		public void map(LongWritable key,Text values,Context context) throws IOException,InterruptedException
		{
			String[] str=values.toString().split(";");
			long sales=Long.parseLong(str[8]);
			long costs=Long.parseLong(str[7]);
			long loss=costs-sales;
			String ageLoss=str[2]+","+loss;
			if(loss>0)
			{
			context.write(new Text(str[5]), new Text(ageLoss));
			}
		}
	}
	
	public static class AgePartitioner extends Partitioner<Text, Text>
	{
		public int getPartition(Text key,Text value, int numReduceTasks)
		{
			String[] str=value.toString().split(",");
			String age=str[0];
			if(age.contains("A"))
			{
				return 0;
			}
			else if(age.contains("B"))
			{
				return 1;
			}
			else if(age.contains("C"))
			{
				return 2;
			}
			else if(age.contains("D"))
			{
				return 3;
			}
			else if(age.contains("E"))
			{
				return 4;
			}
			else if(age.contains("F"))
			{
				return 5;
			}
			else if(age.contains("G"))
			{
				return 6;
			}
			else if(age.contains("H"))
			{
				return 7;
			}
			else if(age.contains("I"))
			{
				return 8;
			}
			else if(age.contains("J"))
			{
				return 9;
			}
			else if(age.contains("K"))
			{
				return 10;
			}
			return numReduceTasks;
		}
	}

	
	public static class MyReducer1 extends Reducer<Text,Text,Text,Text>
	{
		public void reduce(Text key,Iterable<Text> values,Context context) throws IOException,InterruptedException
		{
			long sum=0;
			String age="";
			String ageTot="";
			for(Text val:values)
			{
				String[] str=val.toString().split(",");
				age=str[0];
				long loss=Long.parseLong(str[1]);
				sum=sum+loss;
				ageTot=key+"\t"+age;
			}
			String s=String.format("%d",sum);
			context.write(new Text(s), new Text(ageTot));
		}
	}
	
	public static class MyMapper2 extends Mapper<LongWritable, Text, LongWritable, Text>
	{
		public void map(LongWritable key,Text values,Context context) throws IOException, InterruptedException
		{
			String[] str=values.toString().split("\t");
			String total=str[0];
			String pidAge=str[1]+"\t"+str[2];
			context.write(new LongWritable(Long.parseLong(total)), new Text(pidAge));
		}
	} 
	
	public static class SortPartitioner extends Partitioner<LongWritable, Text>
	{
		public int getPartition(LongWritable key, Text values, int numReduceTasks) 
		{
			String[] str=values.toString().split("\t");
			String age=str[1];
			if(age.contains("A"))
			{
				return 0;
			}
			else if(age.contains("B"))
			{
				return 1;
			}
			else if(age.contains("C"))
			{
				return 2;
			}
			else if(age.contains("D"))
			{
				return 3;
			}
			else if(age.contains("E"))
			{
				return 4;
			}
			else if(age.contains("F"))
			{
				return 5;
			}
			else if(age.contains("G"))
			{
				return 6;
			}
			else if(age.contains("H"))
			{
				return 7;
			}
			else if(age.contains("I"))
			{
				return 8;
			}
			else if(age.contains("J"))
			{
				return 9;
			}
			else if(age.contains("K"))
			{
				return 10;
			}
			return numReduceTasks;
		}
		
	}
	
	public static class MyReducer2 extends Reducer<LongWritable,Text,Text,LongWritable>
	{
		int count=0;
		public void reduce(LongWritable key,Iterable<Text> values,Context context) throws IOException,InterruptedException
		{
			for(Text val:values)
			{
				if(count<5)
				{
				context.write(new Text(val), key);
				count++;
				}
			}	
		}
	}
	
	
	
	public static void main(String[] args) throws Exception
	{
		//MapReduce 1
		
		Configuration conf = new Configuration();
		Job job1 = Job.getInstance(conf, "Highest Amount");
		job1.setJarByClass(TopLossPid.class);
		job1.setMapperClass(MyMapper1.class);
		job1.setPartitionerClass(AgePartitioner.class);
		job1.setReducerClass(MyReducer1.class);
		job1.setNumReduceTasks(11);
		//job1.setMapOutputKeyClass(Text.class);
       // job1.setMapOutputValueClass(Text.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		Path outputPath=new Path("FMapper");
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, outputPath);
		FileSystem.get(conf).delete(outputPath, true);
		job1.waitForCompletion(true);
		//System.exit(job1.waitForCompletion(true) ? 0 : 1);
		
		//MapReduce 2
		
		Job job2 = Job.getInstance(conf, "Highest Amount");
		job2.setJarByClass(TopLossPid.class);
		job2.setMapperClass(MyMapper2.class);
		job2.setPartitionerClass(SortPartitioner.class);
		job2.setReducerClass(MyReducer2.class);
		job2.setNumReduceTasks(11);
		job2.setSortComparatorClass(DecreasingComparator.class);
		job2.setMapOutputKeyClass(LongWritable.class);
	    job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(LongWritable.class);
		FileInputFormat.addInputPath(job2, outputPath);
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));
		System.exit(job2.waitForCompletion(true) ? 0 : 1);
	}
}
