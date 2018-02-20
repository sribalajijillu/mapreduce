import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class ProfitPercentCat 
{
	public static class MapperClass extends Mapper<LongWritable,Text,Text,Text>
	{
		public void map (LongWritable key, Text value, Context context)
		{
			Text outKey =new Text();
			Text outValue=new Text();
			try
			{
				String[] str=value.toString().split(";");
				String category=str[4];
				String salCos=str[7]+","+str[8];
				outKey.set(category);
				outValue.set(salCos);
				context.write(outKey, outValue);
			}
			catch(Exception e)
			{
				System.out.println(e.getMessage());
			}
		}
	}
	public static class ReduceClass extends Reducer<Text,Text,Text,LongWritable>
	{
		public void reduce(Text key,Iterable<Text> value,Context context) throws IOException,InterruptedException
		{
			try {
			long sales=0;
			long cost=0;
			long profit=0;
			long profPercent=0;
			long sum=0;
			for(Text val:value)
			{
				String[] str=val.toString().split(",");
				sales=Long.parseLong(str[1]);
				cost=Long.parseLong(str[0]);
				profit=sales-cost;
				if(profit>0)
				{
					profPercent=(profit*100)/cost;
					sum=sum+profPercent;
				}
			}
			context.write(key, new LongWritable(sum));
			}
			catch(Exception e) 
			{
				System.out.println(e.getMessage());
			}
		}
	}
	public static void main(String args[]) throws Exception	
	{
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf,"Profit Percentage");
		job.setJarByClass(ProfitPercentCat.class);
		job.setMapperClass(MapperClass.class);
		job.setReducerClass(ReduceClass.class);
		job.setNumReduceTasks(1);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

	