import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCountNoReducer {

    public static class WordMapper 
            extends Mapper<Object, Text, Text, NullWritable> {

        private Text wordText = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            
            // Get line and convert to lowercase
            String line = value.toString().toLowerCase();
            StringTokenizer itr = new StringTokenizer(line);
            
            while (itr.hasMoreTokens()) {
                String word = itr.nextToken();
                
                // Example processing: Only output words longer than 3 characters
                if (word.length() > 3) {
                    wordText.set("Processed word: " + word);
                    context.write(wordText, NullWritable.get());
                    System.out.println("Mapper processing: " + word);
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        
        // Performance configurations for mappers
        conf.set("mapreduce.map.memory.mb", "1024");
        conf.set("mapreduce.map.java.opts", "-Xmx819m");
        
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        
        if (otherArgs.length != 2) {
            System.err.println("Usage: wordcount_no_reducer <input> <output>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "word processing - mapper only");
        job.setJarByClass(WordCountNoReducer.class);
        
        // Set mapper class
        job.setMapperClass(WordMapper.class);
        
        // Important: Set number of reducers to 0
        job.setNumReduceTasks(0);
        
        // Set output key and value classes
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.out.println("Starting Mapper-only Job...");
        System.out.println("Input Path: " + otherArgs[0]);
        System.out.println("Output Path: " + otherArgs[1]);
        System.out.println("This job will only process words longer than 3 characters");
        System.out.println("No reducers will be used - direct mapper output");

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}