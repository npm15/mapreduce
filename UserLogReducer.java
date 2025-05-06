package mrLogFile_demo;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class UserLogReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable result = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;

        // Iterate through the list of values (log counts for the user)
        for (IntWritable val : values) {
            sum += val.get();  // Add the count
        }

        // Set the result to the sum and write it to the context
        result.set(sum);
        context.write(key, result);  // Emit (user, total count)
    }
}

