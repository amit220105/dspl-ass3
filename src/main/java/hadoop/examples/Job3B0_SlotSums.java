package hadoop.examples;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Input (Job3A):
 *  p \t slot \t w \t mi \t sumMI
 * Output:
 *  p \t slot \t sumMI
 */
public class Job3B0_SlotSums {

    public static class M extends Mapper<LongWritable, Text, Text, Text> {
        private final Text k = new Text();
        private final Text v = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;
            String[] p = line.split("\t");
            if (p.length != 5) return;

            String path = p[0];
            String slot = p[1];
            String sum  = p[4];

            k.set(path + "\t" + slot);
            v.set(sum);
            ctx.write(k, v);
        }
    }

    public static class R extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {
            // sumMI constant for given (p,slot) => take first
            for (Text v : values) {
                ctx.write(key, v);
                return;
            }
        }
    }

    public static void main(String[] args) throws Exception {
        int off = 0;
        if (args.length >= 3 && args[0].contains(".") && !args[0].startsWith("s3://")) off = 1;
        if (args.length < off + 2) {
            System.err.println("Usage: Job3B0_SlotSums <job3aOut> <job3b0Out>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Job3B0 - Slot sums");
        job.setJarByClass(Job3B0_SlotSums.class);

        job.setMapperClass(M.class);
        job.setReducerClass(R.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[off]));
        FileOutputFormat.setOutputPath(job, new Path(args[off + 1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
