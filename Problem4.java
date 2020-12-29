/**
 * Problem4.java
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


public class Problem4 {

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

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "problem 4");

        // Specifies the name of the outer class.
        job.setJarByClass(Problem4.class);


        /* CHANGE THE CLASS NAMES AS NEEDED IN THE METHOD CALLS BELOW */

        // Specifies the names of the mapper and reducer classes.
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        // Sets the type for the keys output by the mapper and reducer.
        job.setOutputKeyClass(Text.class);

        // Sets the type for the values output by the mapper and reducer,
        // although we can change the mapper's type below.
        job.setOutputValueClass(LongWritable.class);

        // Sets the type for the keys output by the mapper.
        // Not needed here because both the mapper and reducer's output keys 
        // have the same type, but you can uncomment it as needed
        // and pass in the appropriate type.
        //   job.setMapOutputKeyClass(Text.class);

        // Sets the type for the values output by the mapper.
        // This is needed because it is different than the type specified
        // by job.setOutputValueClass() above. 
        // If the mapper and reducer output values of the same type, 
        // you can comment out or remove this line.
        job.setMapOutputValueClass(LongWritable.class);


        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}