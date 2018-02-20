import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class AmountCalc {
	
	public static class MapClass extends Mapper<LongWritable,Text,Text,Text>
	   {
	      public void map(LongWritable key, Text value, Context context)
	      {	    	  
	         try{
	            String[] str = value.toString().split(";");	 
	            String name="Kalaikko";
	            String values=str[0]+","+str[1]+","+str[8];
	            context.write(new Text(name),new Text(values));
	         }
	         catch(Exception e)
	         {
	            System.out.println(e.getMessage());
	         }
	         
	      }
	   }
	
	  public static class ReduceClass extends Reducer<Text,Text,Text,Text>
	   {
		  	public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
		      	long maxAmount=0,sa=0;
		      	String details="";
		      	 for (Text val : values)
		         {       	
		        	 String[] str1 = val.toString().split(",");
		        	 LongWritable s=new LongWritable(Integer.parseInt(str1[2]));
		        	 sa=s.get();
		        	 if(sa>maxAmount)
		        	 {
		        		 maxAmount=sa;
		        		 details=str1[0]+","+str1[1]+","+maxAmount;
		        	 }
		         }
		         		         		      		      
		     
			context.write(key,new Text(details));
		      //context.write(key, new LongWritable(sum));
		      
		    }
	   }
	  public static void main(String[] args) throws Exception {
		    Configuration conf = new Configuration();
		    //conf.set("name", "value")
		    //conf.set("mapreduce.input.fileinputformat.split.minsize", "134217728");
		    Job job = Job.getInstance(conf, "Highest Amount");
		    job.setJarByClass(AmountCalc.class);
		    job.setMapperClass(MapClass.class);
		    //job.setCombinerClass(ReduceClass.class);
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