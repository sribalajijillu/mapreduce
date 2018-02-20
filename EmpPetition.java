import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class EmpPetition 
{
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text> 
	{
		public void map(LongWritable key, Text values, Context con) throws IOException, InterruptedException
		{
			String[] str = values.toString().split("\t");
			con.write(new Text(str[2]), new Text(values));
		}
	}
	public static class YearPartitioner extends Partitioner<Text, Text>
	{

		public int getPartition(Text key, Text values, int numReduceTasks) 
		{
			String[] str = values.toString().split("\t");
			long year = Long.parseLong(str[7]);
			if(year==2011)
			{
				return 0;
			}
			else if(year==2012)
			{
				return 1;
			}
			else if(year==2013)
			{
				return 2;
			}
			else if(year==2014)
			{
				return 3;
			}
			else if(year==2015)
			{
				return 4;
			}
			else
			{
				return 5;
			}
		}		
	}
	public static class ReduceClass extends Reducer<Text,Text,NullWritable,Text>
	{
		private TreeMap<Long, Text> tm = new TreeMap<Long, Text>();
		public void reduce(Text key, Iterable<Text> values, Context con) throws IOException, InterruptedException
		{
			long count=0;
			String year="";
			for(Text val:values)
			{
				String[] str = val.toString().split("\t");
				year = str[7];
				count++;
			}
			String myValue = year+"\t"+key+"\t"+count;
			tm.put(new Long(count), new Text(myValue));
			if(tm.size()>5)
			{
				tm.remove(tm.firstKey());
			}
		}
		protected void cleanup(Context con) throws IOException, InterruptedException
		{
			for(Text t:tm.descendingMap().values())
			{
				con.write(NullWritable.get(), t);
			}
		}
	}
	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, " Top petition");
		job.setJarByClass(EmpPetition.class);
		job.setMapperClass(MapClass.class);
		job.setPartitionerClass(YearPartitioner.class);
		job.setReducerClass(ReduceClass.class);
		job.setNumReduceTasks(6);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
