
package edu.gatech.cse6242;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
public class Q4b {
  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{
   // private final static IntWritable plus = new IntWritable(1);

    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {

      String line = value.toString();
      String[] lines = line.split("\t");
      String pc = lines[2].toString();
      String tf = lines[3].toString();
      Integer one = 1;

      word.set(pc);
	context.write(word,new Text(one+","+tf));   
    }
  }
  public static class IntSumReducer
       extends Reducer<Text,Text,Text,Text> {
  
	
    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {

      
	double r = 0;
	double sum = 0;
	int count = 0;
      for (Text val : values) {
	String a = val.toString();
	String[] lines = a.split(",");
	
	count +=  Integer.parseInt(lines[0]);
        sum += Double.parseDouble(lines[1]);
	
      }

	r =(sum/count);
	String af = String.format("%.2f",r);
	
      context.write(key, new Text(af));
    }
  }
 
 

  public static void main(String[] args) throws Exception {
    
    final String gtid = "yxiao351";
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "job");

    job.setJarByClass(Q4b.class);
    job.setMapperClass(TokenizerMapper.class);
   // job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));

    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
