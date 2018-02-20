import java.io.*;
import java.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class WordCount {
	
	public static class MapClass extends Mapper<LongWritable,Text,Text,IntWritable>
	   {
		int one =1;
		//private IntWritable one = new IntWritable(1);
		private Text word=new Text();
	      public void map(LongWritable key, Text value, Context context)
	      {	 
	    	  try
	    	  {
	         StringTokenizer st =new StringTokenizer(value.toString());
	         while(st.hasMoreTokens())
	         {
	        String myWord=st.nextToken().toLowerCase();
	        word.set(myWord);
	         context.write(word,new IntWritable(one));
	         }
	    	  }
	    	  catch(Exception e){
	    		  System.out.println(e.getMessage());
	    	  }
	      }
	   }
	
	  public static class ReduceClass extends Reducer<Text,IntWritable,Text,IntWritable>
	   {
		  	public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
		      	int sum=0;
		 
		      	 for (IntWritable val : values)
		         {       	
		        	 sum+=val.get();
		         }
		       	         		     
		     context.write(key,new IntWritable(sum));
		      //context.write(key, new LongWritable(sum));
		      
		    }
	   }
	  public static void main(String[] args) throws Exception {
		    Configuration conf = new Configuration();
		    //conf.set("name", "value")
		    //conf.set("mapreduce.input.fileinputformat.split.minsize", "134217728");
		    Job job = Job.getInstance(conf, "Highest Amount");
		    job.setJarByClass(WordCount.class);
		    job.setMapperClass(MapClass.class);
		    //job.setCombinerClass(ReduceClass.class);
		    job.setReducerClass(ReduceClass.class);
		    job.setNumReduceTasks(1);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(IntWritable.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		  }
}
