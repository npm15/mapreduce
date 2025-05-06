package mrLogFile_demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class UserLogDriver {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: UserLogDriver <input path> <output path>");
            System.exit(-1);
        }

        // Create a configuration object for the job
        Configuration conf = new Configuration();

        // Create the Job instance
        Job job = Job.getInstance(conf, "MaxLoggedUsers");

        // Set the jar by class to identify the location of the jar
        job.setJarByClass(UserLogDriver.class);

        // Set Mapper and Reducer classes
        job.setMapperClass(UserLogMapper.class);
        job.setReducerClass(UserLogReducer.class);

        // Set output key and value types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Set input and output formats
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Wait for the job to complete
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

