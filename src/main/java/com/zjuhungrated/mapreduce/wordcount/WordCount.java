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


/**
 * WordCount
 * <p>
 * MapReduce WordCount的简单实例
 */

public class WordCount {

    private static final String IN = "hdfs://localhost:9000/wordcount/input/test.txt";
    private static final String OUT = "hdfs://localhost:9000/wordcount/output/";

    public static void main(String[] args) throws Exception {

        System.setProperty("HADOOP_USER_NAME", "hungrated");

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000");
        conf.set("yarn.resourcemanager.hostname", "localhost");

        FileSystem fs = FileSystem.get(conf);

        Job job = Job.getInstance(conf, "Demo");
        job.setJarByClass(WordCount.class);

        job.setMapperClass(TokenizerMapper.class);

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

    /**
     * TokenizerMapper
     * <p>
     * 对文档进行Map操作类
     */

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        /**
         * map
         * <p>
         * 重写map方法，将输入文档做拆词处理并输出给Reducer
         *
         * @param key     关键词
         * @param value   待处理文本
         * @param context 将输出的上下文
         * @throws IOException          当读文件错误抛出该异常
         * @throws InterruptedException 当被中断时抛出该异常
         */
        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] words = value.toString().split(" ");
            for (String word : words) {
                context.write(new Text(word), new IntWritable(1));
            }
        }
    }

    /**
     * IntSumReducer
     * <p>
     * 对Map结果进行Reduce操作类
     */

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        /**
         * reduce
         * <p>
         * 重写reduce方法，计算每个词出现次数总和
         *
         * @param key     关键词
         * @param values  文本数组
         * @param context 将输出的上下文
         * @throws IOException          当读文件错误抛出该异常
         * @throws InterruptedException 当被中断时抛出该异常
         */
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
