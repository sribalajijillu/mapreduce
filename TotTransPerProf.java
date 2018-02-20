import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.LongWritable.DecreasingComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TotTransPerProf {
	
	public static class CustMapper extends Mapper<LongWritable,Text,Text,Text>
	{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String valueArr[] = value.toString().split(",");
			String custID = valueArr[0];
			String custOcc = valueArr[4];
			String c_custOcc = "c" + "," + custOcc;
			context.write(new Text(custID), new Text(c_custOcc));
		}
	}

	public static class StoreMapper extends Mapper<LongWritable,Text,Text,Text>
	{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String strValue = value.toString();
			String[] valueArr = strValue.split(",");
			String custId = valueArr[2];
			String orderPrice = valueArr[3];
			String s_price = "s" + "," + orderPrice;
			context.write(new Text(custId), new Text(s_price));
		}
	}

	public static class MyReducer1 extends Reducer<Text,Text,Text,DoubleWritable>
	{
		public void reduce(Text key,Iterable<Text> value, Context context) throws IOException, InterruptedException
		{
			//int occCount = 0;
			String occ = "unknown";
			double tot = 0.0;
			//String res = null;
			for(Text val : value)
			{
				String valArr[] = val.toString().split(",");
				String marker = valArr[0];
				if(marker.equals("s"))
				{
					double orderPrice = Double.parseDouble(valArr[1]);
					//occCount++;
					tot+=orderPrice;
				}
				else if(marker.equals("c"))
				{
					occ = valArr[1];
				}
			}
			//res = String.valueOf(occCount) + "--" + String.valueOf(tot);
			context.write(new Text(occ), new DoubleWritable(tot));
		}
	}
	
	public static class Mapper2 extends Mapper<LongWritable,Text,Text,DoubleWritable>
	{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String[] valueArr = value.toString().split("\t");
			context.write(new Text(valueArr[0]), new DoubleWritable(Double.parseDouble(valueArr[1])));
		}
	}

	public static class Reducer2 extends Reducer<Text,DoubleWritable,DoubleWritable,Text>
	{
		@Override
		public void reduce(Text key, Iterable<DoubleWritable> value, Context context) throws IOException, InterruptedException
		{
			double tot = 0.0;
			for(DoubleWritable val : value)
			{
				tot+=val.get();
			}
			context.write(new DoubleWritable(tot), key);
		}
	}

	public static class SortMapper extends Mapper<LongWritable,Text,DoubleWritable,Text>
	{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String[] valueArr = value.toString().split("\t");
			context.write(new DoubleWritable(Double.parseDouble(valueArr[0])), new Text(valueArr[1]));
		}
	}

	public static class SortReducer extends Reducer<DoubleWritable,Text,Text,DoubleWritable>
	{
		public void reduce(DoubleWritable key, Iterable<Text> value, Context context) throws IOException, InterruptedException
		{
			for(Text val : value)
			{
				context.write(new Text(val), key);
			}
		}
	}
	

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException 
	{
		Configuration conf = new Configuration();
		Job job1 = Job.getInstance(conf,"Occupation - totaltxn ");
		job1.setJarByClass(TotTransPerProf.class);
		job1.setReducerClass(MyReducer1.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(DoubleWritable.class);
		MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, CustMapper.class);
		MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, StoreMapper.class);
		Path outputPath1 = new Path("FirstMapper");
		FileOutputFormat.setOutputPath(job1, outputPath1);
		FileSystem.get(conf).delete(outputPath1, true);
		job1.waitForCompletion(true);

		Job job2 = Job.getInstance(conf,"totaltxn - Occupation");
		job2.setJarByClass(TotTransPerProf.class);
		job2.setMapperClass(Mapper2.class);
		job2.setReducerClass(Reducer2.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(DoubleWritable.class);
		job2.setOutputKeyClass(DoubleWritable.class);
		job2.setOutputValueClass(Text.class);
		Path outputPath2 = new Path("SecondMapper");
		FileInputFormat.addInputPath(job2,outputPath1);
		FileOutputFormat.setOutputPath(job2, outputPath2);
		FileSystem.get(conf).delete(outputPath2, true);
		job2.waitForCompletion(true);

		Job job3 = Job.getInstance(conf,"Per Occupation - totalTxn");
		job3.setJarByClass(TotTransPerProf.class);
		job3.setMapperClass(SortMapper.class);
		job3.setReducerClass(SortReducer.class);
		job3.setSortComparatorClass(DecreasingComparator.class);
		job3.setMapOutputKeyClass(DoubleWritable.class);
		job3.setMapOutputValueClass(Text.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job3, outputPath2);
		FileOutputFormat.setOutputPath(job3, new Path(args[2]));
		FileSystem.get(conf).delete(new Path(args[2]), true);
		System.exit(job3.waitForCompletion(true) ? 0 : 1);
		
	
	}
		
	}

	
	