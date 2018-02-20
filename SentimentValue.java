import java.io.*;
import java.util.*;
import java.net.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class SentimentValue 
{
	public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

		private Map<String, String> abMap = new HashMap<String, String>();
		private final static IntWritable total_value = new IntWritable();
		private Text word = new Text();
		String myWord = "";
		int myValue = 0;
		
		protected void setup(Context context) throws java.io.IOException, InterruptedException{
			
			super.setup(context);

		    URI[] files = context.getCacheFiles(); // getCacheFiles returns null

		    Path p = new Path(files[0]);
		
		    FileSystem fs = FileSystem.get(context.getConfiguration());		    
		
			if (p.getName().equals("AFINN.txt")) {
					//BufferedReader reader = new BufferedReader(new FileReader(p.toString()));
				BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(p)));
					
					String line = reader.readLine();
					while(line != null) {
						String[] tokens = line.split("\t");
						String diction_word = tokens[0];
						String diction_value = tokens[1];
						abMap.put(diction_word, diction_value);
						line = reader.readLine();
					}
					reader.close();
				}
			
			if (abMap.isEmpty()) {
				throw new IOException("MyError:Unable to load dictionary data.");
			}
		}
		
		
		public void map(LongWritable key, Text value, Context context
                  ) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) 
			{
				myWord = itr.nextToken().toLowerCase();
	        	if (abMap.get(myWord)!=null)
	        	{
	        		myValue = Integer.parseInt(abMap.get(myWord));
	        		if (myValue>0)
	        		{
	        			myWord = "positive";
	        		}
	        		if (myValue<0)
	        		{
	        			myWord = "negative";
	        			myValue = myValue * -1;
	        		}
	        	}
	        	else
	        	{
	        		myWord = "positive";
	        		myValue = 0;
	        	}
				
	        	word.set(myWord);
	        	total_value.set(myValue);
				context.write(word,total_value);
			}
		}
	}

	public static class IntSumReducer
      extends Reducer<Text,IntWritable,NullWritable,Text> 
	{
		int pos_total = 0 ; //variables
		int neg_total = 0 ;
		double sentpercent = 0.00;
		
		public void reduce(Text key, Iterable<IntWritable> values,
                      Context context
                      ) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) 
			{
				sum += val.get();
			}
     
			if (key.toString().equals("positive"))
			{
				pos_total=sum;
			}
			
			if (key.toString().equals("negative"))			{
				neg_total=sum;
			}
		
		}
		protected void cleanup(Context context) throws IOException,
		InterruptedException 
		{
		sentpercent = (((double)pos_total) - ((double)neg_total))/(((double)pos_total) + ((double)neg_total)) * 100;
		String str = "Sentiment percent for the given text is " + String.format("%f", sentpercent);
		context.write(NullWritable.get(), new Text(str));
			//context.write(NullWritable.get(), new Text("hello"));
		}
	}
	
	  public static void main(String[] args) throws Exception {
		    Configuration conf = new Configuration();
		    Job job = Job.getInstance(conf, "sentiment %");
		    job.setJarByClass(SentimentValue.class);
		    job.setMapperClass(TokenizerMapper.class);
		    //job.addCacheFile(new Path("AFINN.txt").toUri());
		    //job.setCombinerClass(IntSumReducer.class);
		    job.addCacheFile(new Path(args[0]).toUri());
		    job.setReducerClass(IntSumReducer.class);
		    //job.setNumReduceTasks(0);
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(IntWritable.class);
		    job.setOutputKeyClass(NullWritable.class);
		    job.setOutputValueClass(Text.class);
		    FileInputFormat.addInputPath(job, new Path(args[1]));
		    FileOutputFormat.setOutputPath(job, new Path(args[2]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		  }
}
