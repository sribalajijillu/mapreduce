import java.io.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class MovieRating {
//5   
    public static class MapClass extends Mapper<LongWritable,Text,Text,Text>
       {
          public void map(LongWritable key, Text value, Context context)
          {             
             try{
                 String s="Rating greater than 3.9";
                    context.write(new Text(s), new Text(value));
             }
             catch(Exception e)
             {
                System.out.println(e.getMessage());
             }
          }
       }
   
      public static class ReduceClass extends Reducer<Text,Text,Text,IntWritable>
       {
            //private IntWritable result = new IntWritable();
           
            public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
              int count = 0;
              //int empty=0; 
              for (Text val : values)
              { String[] str=val.toString().split(",");
              if(str[3].isEmpty())
              {
            	  continue;
              }
              else {
                   double rating=Double.parseDouble(str[3]);
                   if(rating>3.9)
                   {
                       count++;
                   }
              }
              }
                
              //result.set(count);             
              context.write(key,new IntWritable(count));
              //context.write(key, new LongWritable(sum));
             
            }
       }
      public static void main(String[] args) throws Exception {
            Configuration conf = new Configuration();
            //conf.set("name", "value")
            //conf.set("mapreduce.input.fileinputformat.split.minsize", "134217728");
            Job job = Job.getInstance(conf, "Volume Count");
            job.setJarByClass(MovieRating.class);
            job.setMapperClass(MapClass.class);
            //job.setCombinerClass(ReduceClass.class);
            job.setReducerClass(ReduceClass.class);
            job.setNumReduceTasks(1);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
          }
}