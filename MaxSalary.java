import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class MaxSalary {
	
	public static class MapClass extends Mapper<LongWritable,Text,Text,Text>
	   {
	      public void map(LongWritable key, Text value, Context context)
	      {	    	  
	         try{
	            String[] str = value.toString().split(",");	 
	            String gender=str[3];
	            context.write(new Text(gender),new Text(value));
	         }
	         catch(Exception e)
	         {
	            System.out.println(e.getMessage());
	         }
	         
	      }
	   }
	
	  public static class ReduceClass extends Reducer<Text,Text,Text,IntWritable>
	   {
		  	public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
		      	int maxSal=0;
		      	String myKey="";
		      	 for (Text val : values)
		         {       	
		        	 String[] str = val.toString().split(",");
		        	 if(Integer.parseInt(str[4])>maxSal)
		        	 {
		        		 maxSal=Integer.parseInt(str[4]);
		        		 myKey=str[3]+","+str[1]+","+str[2];
		        	 }
		         }
		         		         		      		      
		     
			context.write(new Text(myKey),new IntWritable(maxSal));
		      //context.write(key, new LongWritable(sum));
		      
		    }
	   }
	  public static class CaderPartitioner extends Partitioner < Text, Text >
	   {
	      @Override
	      public int getPartition(Text key, Text value, int numReduceTasks)
	      {
	         String[] str = value.toString().split(",");
	         int age = Integer.parseInt(str[2]);


	         if(age<=20)
	         {
	            return 0 % numReduceTasks;
	         }
	         else if(age>20 && age<=30)
	         {
	            return 1 % numReduceTasks ;
	         }
	         else
	         {
	            return 2 % numReduceTasks;
	         }
	      }
	   }
	  public static void main(String[] args) throws Exception {
		    Configuration conf = new Configuration();
		    //conf.set("name", "value")
		    //conf.set("mapreduce.input.fileinputformat.split.minsize", "134217728");
		    Job job = Job.getInstance(conf,"Top Salaried Employees");
		    job.setJarByClass(MaxSalary.class);
		    job.setMapperClass(MapClass.class);
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(Text.class);
		    job.setPartitionerClass(CaderPartitioner.class);
		    //job.setCombinerClass(ReduceClass.class);
		    job.setReducerClass(ReduceClass.class);
		    job.setNumReduceTasks(3);
		    
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(IntWritable.class);
		    job.setInputFormatClass(TextInputFormat.class);
		    job.setOutputFormatClass(TextOutputFormat.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		  }
}