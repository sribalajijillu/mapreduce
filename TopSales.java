//retail Store (B)
import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.LongWritable.DecreasingComparator;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class TopSales 
{
	//MapReduce 1
	
	public static class MyMapper1 extends Mapper<LongWritable, Text, Text, LongWritable>
	{
		public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException
		{
			String[] str = values.toString().split(";");
			String prodId = str[5];
			long sales = Long.parseLong(str[8]);
			context.write(new Text(prodId), new LongWritable(sales));
		}
	}
	
	public static class MyReducer1 extends Reducer<Text, LongWritable, Text, LongWritable>
	{
		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException
		{
			long sum=0;
			for(LongWritable val:values)
			{
				sum=sum+val.get();
			}
			context.write(key, new LongWritable(sum));
		}
	}
	
	//MapReduce 2
	
	public static class MyMapper2 extends Mapper<LongWritable, Text, Text, Text>
	{
		public void map(LongWritable key, Text values, Context context) throws IOException,InterruptedException
		{
			String[] str = values.toString().split("\t");
			context.write(new Text(str[1]), new Text(str[0]));
		}
	}
	
	public static class MyReducer2 extends Reducer<Text, Text, Text, Text>
	{
		int count=0;
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			for(Text val:values)
			{
				if(count<10)
				{
				context.write(new Text(val), key);
				count++;
				}
			}
		}
	}
	
	public static void main(String[] args) throws Exception
	{
		// MapReduce 1
		
		Configuration conf = new Configuration();
		//conf.set("mapreduce.output.fileoutputformat.seperator", ",");
		Job job1 = Job.getInstance(conf,"Top Sales");
		job1.setJarByClass(TopSales.class);
		job1.setMapperClass(MyMapper1.class);
		job1.setReducerClass(MyReducer1.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(LongWritable.class);
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		Path outputPath =new Path("1stMapper");
		FileOutputFormat.setOutputPath(job1, outputPath);
		FileSystem.get(conf).delete(outputPath, true);
		job1.waitForCompletion(true);
		
		//MapReduce 2
		
		Job job2 = Job.getInstance(conf,"Sort Sales");
		job2.setJarByClass(TopSales.class);
		job2.setMapperClass(MyMapper2.class);
		job2.setReducerClass(MyReducer2.class);
		job2.setSortComparatorClass(DecreasingComparator.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job2, outputPath);
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));
		System.exit(job2.waitForCompletion(true) ? 0 : 1);
	}
}
