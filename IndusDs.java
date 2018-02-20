import java.io.IOException;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class IndusDs 
{
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text>
	{
		private Text myKey = new Text();
		private Text myValue = new Text();
		public void map(LongWritable key, Text values, Context con) throws IOException, InterruptedException
		{
			String[] str = values.toString().split("\t");
			myKey.set(str[3]);
			myValue.set(str[1]+"\t"+str[4]);
			con.write(myKey, myValue);
		}
	}
	public static class ReduceClass extends Reducer<Text, Text, NullWritable, Text>
	{
		public TreeMap<Long, Text> tm = new TreeMap<Long, Text>();
		public void reduce(Text key, Iterable<Text> values, Context con) throws IOException, InterruptedException
		{
			long count=0;
			String jobTitle="";
			String caseStatus="";
			String KeyVal = "";
			for(Text val:values)
			{
				String[] str = val.toString().split("\t");
				caseStatus = str[0];
				jobTitle = str[1];
				if((caseStatus.equals("CERTIFIED")) && (jobTitle.equals("DATA SCIENTIST")))
				{
					count++;
					KeyVal = key+"\t"+jobTitle;
				}
			}
			//String myVal = key+"\t"+jobTitle+"\t"+count;
			String myVal = KeyVal+"\t"+count;
			tm.put(new Long(count), new Text(myVal));
			if(tm.size()>1)
			{
				tm.remove(tm.firstKey());
			}
			//con.write(key, new Text(myVal));
		}
		public void cleanup(Context con) throws IOException, InterruptedException
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
		Job job = Job.getInstance(conf,"Most Data Scientist");
		job.setJarByClass(IndusDs.class);
		job.setMapperClass(MapClass.class);
		job.setReducerClass(ReduceClass.class);
		job.setNumReduceTasks(1);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
