import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class YearApp 
{
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text> 
	{
		public void map(LongWritable key, Text values, Context con) throws IOException, InterruptedException
		{
			String[] str = values.toString().split("\t");
			con.write(new Text(str[7]), new Text(values));
		}
	}
	
	public static class ReduceClass extends Reducer<Text, Text, Text, LongWritable>
	{
		public void reduce(Text key, Iterable<Text> values, Context con) throws IOException, InterruptedException
		{
			long count=0;
			for(Text val:values)
			{
				count++;
			}
			con.write(key, new LongWritable(count));
		}
	}
	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf,"Year Application");
		job.setJarByClass(YearApp.class);
		job.setMapperClass(MapClass.class);
		job.setReducerClass(ReduceClass.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setNumReduceTasks(1);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}