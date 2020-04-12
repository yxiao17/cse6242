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


public class Q1 {
/* TODO: Update variable below with your gtid */
  final String gtid = "yxiao351";
  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{
 //   private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
     // StringTokenizer itr = new StringTokenizer(value.toString());
     // while (itr.hasMoreTokens()) {
     //   word.set(itr.nextToken());
     //   context.write(word, one);
     // }
	
	String line =value.toString();
	String[] lines = line.split(",");
	String pickup = lines[0].toString();
        Double dis =Double.parseDouble(lines[2]);	
	Double fare =Double.parseDouble(lines[3]);
	Integer one = 1;
	if (dis != 0 && fare >= 0) {
	word.set(pickup);
//	System.out.println(o);
	context.write(word, new Text(one+","+fare));
//	System.out.println(context);
	}
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,Text,Text,Text> {
//    private IntWritable result = new IntWritable();
//	 NumberFormat format = new DecimalFormatter("#.##");
    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      double f = 0;
    
      for (Text val : values) {
	String a = val.toString();
	String[] total = a.split(",");
  	String num = total[0];
	Double tfare = Double.parseDouble(total[1]);	
        sum += Integer.parseInt(num);
	f += tfare;
      }
	String totalfare = String.format("%,.2f",f);
      context.write(key, new Text(sum+","+totalfare));
    }
  }	
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Q1");
    /* TODO: Needs to be implemented */
    job.setJarByClass(Q1.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
