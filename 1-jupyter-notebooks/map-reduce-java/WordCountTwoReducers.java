import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.Partitioner;

public class WordCountTwoReducers {

    // Custom Partitioner to distribute words based on their first character
    public static class WordPartitioner extends Partitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numReduceTasks) {
            // Words starting with a-m go to first reducer, n-z to second reducer
            String word = key.toString().toLowerCase();
            if (word.isEmpty()) return 0;
            
            char firstChar = word.charAt(0);
            if (firstChar >= 'a' && firstChar <= 'm') {
                return 0;
            } else {
                return 1;
            }
        }
    }

    public static class TokenizerMapper 
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                String token = itr.nextToken().toLowerCase();
                word.set(token);
                context.write(word, one);
                System.out.println("Mapper processing word: " + token);
            }
        }
    }

    public static class IntSumReducer 
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
            
            // Log which reducer is processing this word
            System.out.println("Reducer " + context.getTaskAttemptID().getTaskID().getId() + 
                             " processing word: " + key.toString() + " = " + sum);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        
        // Performance configurations
        conf.set("mapreduce.map.memory.mb", "1024");
        conf.set("mapreduce.reduce.memory.mb", "1024");
        conf.set("mapreduce.map.java.opts", "-Xmx819m");
        conf.set("mapreduce.reduce.java.opts", "-Xmx819m");
        
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        
        if (otherArgs.length != 2) {
            System.err.println("Usage: wordcount <input> <output>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "word count with two reducers");
        job.setJarByClass(WordCountTwoReducers.class);
        
        // Set two reducers
        job.setNumReduceTasks(2);
        
        // Set custom partitioner
        job.setPartitionerClass(WordPartitioner.class);
        
        // Set mapper and reducer
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.out.println("Starting Job with two reducers...");
        System.out.println("Input Path: " + otherArgs[0]);
        System.out.println("Output Path: " + otherArgs[1]);
        System.out.println("Words a-m will go to reducer 1");
        System.out.println("Words n-z will go to reducer 2");

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}