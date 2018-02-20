import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class YearCal {
	
	public static class MapClass extends Mapper<LongWritable,Text,Text,Text>
	   {
	      public void map(LongWritable key, Text value, Context context)
	      {	    	  
	         try{
	            String[] str = value.toString().split(",");	 
	            context.write(new Text(str[2]), new Text(str[1]));
	         }
	         catch(Exception e)
	         {
	            System.out.println(e.getMessage());
	         }
	         
	      }
	   }
	
	  public static class ReduceClass extends Reducer<Text,Text,Text,LongWritable>
	   {
		  	public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
		      	long count=0;
		      	 for (Text val : values)
		         {       	
		      		 count++;
		         }
		         		         		      		      
		     
			context.write(key,new LongWritable(count));
		      
		    }
	   }
	  public static void main(String[] args) throws Exception {
		    Configuration conf = new Configuration();
		    Job job = Job.getInstance(conf, "Year Count");
		    job.setJarByClass(YearCal.class);
		    job.setMapperClass(MapClass.class);
		    job.setReducerClass(ReduceClass.class);
		    job.setNumReduceTasks(1);
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(Text.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(LongWritable.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		  }
}