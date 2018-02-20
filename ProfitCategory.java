import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class ProfitCategory {
	
	public static class MapClass extends Mapper<LongWritable,Text,Text,LongWritable>
	   {
	      public void map(LongWritable key, Text value, Context context)
	      {	    	  
	         try{
	            String[] str = value.toString().split(";");	 
	            
	            long sales=Long.parseLong(str[8]);
	            long cost=Long.parseLong(str[7]);
	            long profit=sales-cost;
	            if(profit>0)
	            {
	            context.write(new Text(str[4]),new LongWritable(profit));
	            }
	            }
	         catch(Exception e)
	         {
	            System.out.println(e.getMessage());
	         }
	         
	      }
	   }
	
	  public static class ReduceClass extends Reducer<Text,LongWritable,Text,LongWritable>
	   {
		  	public void reduce(Text key, Iterable<LongWritable> values,Context context) throws IOException, InterruptedException {
		      	long sum=0;
		 
		      	 for (LongWritable val : values)
		         {       	
		        	 sum+=val.get();
		         }
		       	         		     
		     context.write(key,new LongWritable(sum));
		      //context.write(key, new LongWritable(sum));
		      
		    }
	   }
	  public static void main(String[] args) throws Exception {
		    Configuration conf = new Configuration();
		    //conf.set("name", "value")
		    //conf.set("mapreduce.input.fileinputformat.split.minsize", "134217728");
		    Job job = Job.getInstance(conf, "Highest Amount");
		    job.setJarByClass(ProfitCategory.class);
		    job.setMapperClass(MapClass.class);
		    //job.setCombinerClass(ReduceClass.class);
		    job.setReducerClass(ReduceClass.class);
		    job.setNumReduceTasks(1);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(LongWritable.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		  }
}
