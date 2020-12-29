/**
 * Problem7.java
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


public class Problem7 {
    /*
     * One possible type for the values that the mapper should output
     */
    public static class IntArrayWritable extends ArrayWritable {
        public IntArrayWritable() {
            super(IntWritable.class);
        }
        
        public IntArrayWritable(IntWritable[] values) {
            super(IntWritable.class, values);
        }

        public int[] toIntArray() {
            Writable[] w = this.get();
            int[] a = new int[w.length];
            for (int i = 0; i < a.length; ++i) {
                a[i] = Integer.parseInt(w[i].toString());
            }
            return a;
        }
    }

    public static class MyMapper extends
    Mapper < Object, Text, Text, IntArrayWritable > 
    {
        public void map(Object key, Text value, Context context) 
            throws IOException, InterruptedException 
        {
            String line = value.toString();

            if (line.contains(";")) {
                String[] fields = line.split(";");
                String[] fields1 = fields[0].split(",");
                String[] friendsList = fields[1].split(",");
                int userId = Integer.parseInt(fields1[0]);
                IntWritable[] writableFriendsList = new IntWritable[friendsList.length];
                
                for (int i = 0; i < friendsList.length; i++) {
                    writableFriendsList[i] = new IntWritable(Integer.parseInt(friendsList[i]));
                }
                
                for (int i = 0; i < friendsList.length; i++) {
                    int friendId = Integer.parseInt(friendsList[i]);
                    if (userId < friendId) {
                        context.write(new Text(userId + "," + friendId), new IntArrayWritable(writableFriendsList));
                    } else {
                        context.write(new Text(friendId + "," + userId), new IntArrayWritable(writableFriendsList));
                    }
                }
            }
        }
    }
    
    public static class MyReducer extends
    Reducer < Text, IntArrayWritable, Text, Text > 
    {
        public void reduce(Text key, Iterable<IntArrayWritable> values, Context context) 
             throws IOException, InterruptedException 
        {
            String[] userIds = key.toString().split(",");
            
            int id1 = Integer.parseInt(userIds[0]);
            int id2 = Integer.parseInt(userIds[1]);
            
            Iterator<IntArrayWritable> iterator = values.iterator();
            int[] friends1 = iterator.next().toIntArray();
            int[] friends2;

            HashSet<Integer> firstPair = new HashSet<Integer>();
            HashSet<Integer> secondPair = new HashSet<Integer>();
            
            if (iterator.hasNext()) {
                friends2 = iterator.next().toIntArray();
            } else {
                friends2 = new int[0];
            }

            if (friends1.length > friends2.length && friends1.length != 0) {
                for (int friend : friends2) {
                    if (friend != id1 && friend != id2) {
                        firstPair.add(friend);
                    }
                }
                
                for (int friend: friends1) {
                    if (firstPair.contains(friend)) {
                        secondPair.add(friend);
                    }
                }
            } else {
                for (int friend : friends1) {
                    if (friend != id1 && friend != id2) {
                        firstPair.add(friend);
                    }
                }
                
                for (int friend: friends2) {
                    if (firstPair.contains(friend)) {
                        secondPair.add(friend);
                    }
                }
            }
            
            String intersection = "";
            for(int friend : secondPair) {
            
               intersection += (intersection==""?"":",") + friend;
            
            }
            
        context.write(key, new Text(intersection));
        }
    }

    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "problem 7");
        job.setJarByClass(Problem7.class);

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //   job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntArrayWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}