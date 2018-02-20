import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class MovieDur {
	
	public static class MapClass extends Mapper<LongWritable,Text,Text,Text>
	   {
	      public void map(LongWritable key, Text value, Context context)
	      {	    	  
	         try{
	        	 String[] str = value.toString().split(",");
	            String s="No. of movies duration>1.5";
	            String str2 = str[1]+","+str[2]+","+str[3]+","+str[4];
	            context.write(new Text(s), new Text(str2));
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
		      		 String[] str=val.toString().split(",");
		      		 if(str[3].isEmpty())
		      		 {
		      			 continue;
		      		 }
		      		 else
		      		 {
		      		 double sec=Double.parseDouble(str[3]);
		      		 if((sec/(60*60))>1.5)
		      		 {
		      			 count++;
		      		 }
		      		 }
		         }
		         		         		      		      
		     
			context.write(key,new LongWritable(count));
		      
		    }
	   }
	  public static void main(String[] args) throws Exception {
		    Configuration conf = new Configuration();
		    Job job = Job.getInstance(conf, "Count B/W Year");
		    job.setJarByClass(MovieDur.class);
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