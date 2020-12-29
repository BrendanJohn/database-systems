/**
 * Problem6.java
 */

import java.io.IOException;
import java.time.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/*
 * interfaces and classes for Hadoop data types that you may
 * need for some or all of the problems from PS 4
 */
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Problem6 {

    public static class MyMapper extends
    Mapper < Object, Text, IntWritable, IntWritable > {
        public void map(Object key, Text value, Context context)
        throws IOException,
        InterruptedException {
  
            String line = value.toString();
            String[] fields = line.split(",");
            int userId = Integer.valueOf(fields[0]);

            if (line.contains(";")) {
                String[] friendsList = line.split(";")[1].split(",");

                context.write(new IntWritable(userId), new IntWritable(friendsList.length));

            }

        }
    }

    public static class MyReducer extends
    Reducer < IntWritable, IntWritable, IntWritable, IntWritable > {
        public void reduce(IntWritable key, Iterable < IntWritable > values,
            Context context) throws IOException,
        InterruptedException {
            int counts = 0;
            for (IntWritable val: values) {
                counts += val.get();
            }
            context.write(key, new IntWritable(counts));

        }
    }

    public static class MyMapper2 extends
    Mapper < Object, Text, Text, Text > {
        public void map(Object key, Text value, Context context)
        throws IOException,
        InterruptedException {

            String resultsFromJob1 = value.toString();
            context.write(new Text("max friends"), new Text(resultsFromJob1));
        }
    }

    public static class MyReducer2 extends
    Reducer < Text, Text, Text, IntWritable > {
        public void reduce(Text key, Iterable < Text > values,
            Context context) throws IOException,
        InterruptedException {           
            int maxNumFriends = 0;
            String userWithMostFriends = "";

            for (Text value: values) {
                String val = value.toString();
                String[] valStringArray = val.split("\t");
                String userId = valStringArray[0];
                int numFriends = Integer.valueOf(valStringArray[1]);
                
                while (numFriends > maxNumFriends) {
                    maxNumFriends = numFriends;
                    userWithMostFriends = userId;
                }
            }

            context.write(new Text(userWithMostFriends),
                new IntWritable(maxNumFriends));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "problem 6");
        job1.setJarByClass(Problem6.class);

        /* CHANGE THE CLASS NAMES AS NEEDED IN THE METHOD CALLS BELOW */
        // See Problem 4.java for comments describing the calls

        job1.setMapperClass(MyMapper.class);
        job1.setReducerClass(MyReducer.class);

        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(IntWritable.class);
        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputValueClass(IntWritable.class);

        job1.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        job1.waitForCompletion(true);

        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "problem 6");
        job2.setJarByClass(Problem6.class);

        /* CHANGE THE CLASS NAMES AS NEEDED IN THE METHOD CALLS BELOW */
        // See Problem 4.java for comments describing the calls

        job2.setMapperClass(MyMapper2.class);
        job2.setReducerClass(MyReducer2.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);

        job2.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        job2.waitForCompletion(true);
    }
}