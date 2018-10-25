package com.zjuhungrated.mapreduce.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCount {

    private static final String IN = "hdfs://localhost:9000/wordcount/input/test.txt";
    private static final String OUT = "hdfs://localhost:9000/wordcount/output/";

    public static void main(String[] args) throws Exception {

        //设置环境变量HADOOP_USER_NAME，其值是root
        //在本机调试
        System.setProperty("HADOOP_USER_NAME", "hungrated");
        //读取配置文件
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000");
        conf.set("yarn.resourcemanager.hostname", "localhost");

        FileSystem fs = FileSystem.get(conf);

        Job job = Job.getInstance(conf, "Demo");
        job.setJarByClass(WordCount.class); //主类

        job.setMapperClass(TokenizerMapper.class);

        //combine过程发生在map方法和reduce方法之间，它将中间结果进行了一次合并。
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setNumReduceTasks(2);

        Path out = new Path(OUT);
        org.apache.hadoop.mapreduce.lib.input.FileInputFormat.setInputPaths(job,
                new Path(IN));
        org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath(job, out);

        if (fs.exists(out)) {
            fs.delete(out, true);
        }
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] words = value.toString().split(" ");
            for (String word : words) {
                context.write(new Text(word), new IntWritable(1));
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable value : values) {
                count += value.get();
            }
            context.write(key, new IntWritable(count));
        }
    }
}
