/**
 * Problem5.java
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


public class Problem5 {

    public static class MyMapper extends Mapper < Object, Text, Text, LongWritable > {
        public void map(Object key, Text value, Context context)
        throws IOException,
        InterruptedException {
            String line = value.toString();
            String[] words = line.split(",");

            for (int i = 0; i < words.length; i++) {
                if (words[i].contains("@")) {
                    String[] emailTemp = words[i].split("@");
                    String a = emailTemp[1];
                    String[] domainList = a.split(";");
                    String emailDomain = domainList[0];
                    context.write(new Text(emailDomain), new LongWritable(1));
                }
            }
        }
    }

    public static class MyReducer extends Reducer < Text, LongWritable, Text, LongWritable > {
        public void reduce(Text key, Iterable < LongWritable > values, Context context)
        throws IOException,
        InterruptedException {
            long counts = 0;
            for (LongWritable val: values) {
                counts += val.get();
            }
            context.write(key, new LongWritable(counts));

        }
    }

    public static class MyMapper2 extends
    Mapper < Object, Text, Text, Text > {
        public void map(Object key, Text value, Context context)
        throws IOException,
        InterruptedException {
            String resultsFromJob1 = value.toString();
            context.write(new Text("domain total"), new Text(resultsFromJob1));
        }
    }

    public static class MyReducer2 extends
    Reducer < Text, Text, Text, LongWritable > {
        public void reduce(Text key, Iterable < Text > values,
            Context context) throws IOException,
        InterruptedException {
            long mostNumUsers = 0;
            String domainMost = "";

            for (Text value: values) {
                String val = value.toString();
                String[] valStringArray = val.split("\t");
                String domain = valStringArray[0];
                long numUsers = Long.valueOf(valStringArray[1]);

                while (numUsers > mostNumUsers) {
                    domainMost = domain;
                    mostNumUsers = numUsers;
                }
            }

            context.write(new Text(domainMost), new LongWritable(mostNumUsers));
        }
    }

    public static void main(String[] args) throws Exception {
        /*
         * First job in a chain of two jobs
         */
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "problem 5");
        job1.setJarByClass(Problem5.class);

        /* CHANGE THE CLASS NAMES AS NEEDED IN THE METHOD CALLS BELOW */
        // See Problem 4.java for comments describing the calls

        job1.setMapperClass(MyMapper.class);
        job1.setReducerClass(MyReducer.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(LongWritable.class);
        //   job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(LongWritable.class);

        job1.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        job1.waitForCompletion(true);


        /*
         * Second job in a chain of two jobs
         */
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "problem 5");
        job2.setJarByClass(Problem5.class);

        /* CHANGE THE CLASS NAMES AS NEEDED IN THE METHOD CALLS BELOW */
        // See Problem 4.java for comments describing the calls

        job2.setMapperClass(MyMapper2.class);
        job2.setReducerClass(MyReducer2.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        //   job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);

        job2.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        job2.waitForCompletion(true);
    }
}