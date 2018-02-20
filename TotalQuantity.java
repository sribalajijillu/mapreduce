import java.io.*;
import java.util.*;
import java.net.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


//import MaxSalary.CaderPartitioner;


public class TotalQuantity {
	
	
	public static class MyMapper extends Mapper<LongWritable,Text, Text, Text> {
        
		
		private Map<String, String> abMap = new HashMap<String, String>();
		private Text outputKey = new Text();
		private Text outputValue = new Text();
		
		protected void setup(Context context) throws java.io.IOException, InterruptedException{
			
			super.setup(context);

			URI[] files = context.getCacheFiles(); // getCacheFiles returns null

		    Path p = new Path(files[0]);
				   
			FileSystem fs = FileSystem.get(context.getConfiguration());		    
		
			if (p.getName().equals("store_master")) {
					//BufferedReader reader = new BufferedReader(new FileReader(p.toString()));
					BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(p)));

					String line = reader.readLine();
					while(line != null) {
						String[] tokens = line.split(",");
						String storeId = tokens[0];
						String state=tokens[2];
						abMap.put(storeId,state);
						line = reader.readLine();
					}
					reader.close();
				}

			
			if (abMap.isEmpty()) {
				throw new IOException("MyError:Unable to load salary data.");
			}
		}

		
        protected void map(LongWritable key, Text value, Context context)
            throws java.io.IOException, InterruptedException {
        	        	
        	String row = value.toString();//reading the data from Employees.txt
        	String[] tokens = row.split(",");
        	String storeId = tokens[0];
        	String state = abMap.get(storeId);
        	String prodId= tokens[1];
        	String quantity=tokens[2];
            String sq=state+","+quantity;
        	outputKey.set(prodId);
        	outputValue.set(sq);
      	  	context.write(outputKey,outputValue);
        }  
}
	public static class ReduceClass extends Reducer<Text,Text,Text,Text>
	{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			int sum=0;
			int a=0;
			String state="";
			for(Text val:values)
			{
				String[] str =val.toString().split(",");
				a=Integer.parseInt(str[1]);
				sum=sum+a;
				state=str[0];
			}
			String tot=state+","+sum;
			context.write(key, new Text(tot));
		}
	}
	
	public static class StatePartitioner extends Partitioner<Text, Text>
	{
		public int getPartition(Text key, Text value, int numReduceTasks)
		{
			String[] str=value.toString().split(",");
			String state=str[0];
			if(state.equalsIgnoreCase("MAH"))
			{
				return 0;
			}
			else if(state.equalsIgnoreCase("KAR"))
			{
				return 1;
			}
			else
			{
				return 2;
			}
			
		}
	}
	
  public static void main(String[] args) 
                  throws IOException, ClassNotFoundException, InterruptedException {
    
	Configuration conf = new Configuration();
	conf.set("mapreduce.output.textoutputformat.separator", ",");
	Job job = Job.getInstance(conf);
    job.setJarByClass(TotalQuantity.class);
    job.setJobName("Map Side Join");
    job.setMapperClass(MyMapper.class);
    job.addCacheFile(new Path(args[1]).toUri());
    job.setPartitionerClass(StatePartitioner.class);
    job.setReducerClass(ReduceClass.class);
    job.setNumReduceTasks(3);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    
    job.waitForCompletion(true);
        
  }
}