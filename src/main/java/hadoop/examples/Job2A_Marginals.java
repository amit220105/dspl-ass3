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


public class Job2A_Marginals {


    public static class MapperClass extends Mapper<LongWritable, Text, Text, LongWritable> {
            private final Text outKey = new Text();
            private final LongWritable outValue = new LongWritable();


        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;
            String[] parts = line.split("\t");
            if (parts.length != 4) return; // invalid line
            String p = parts[0];
            String slot = parts[1];
            String w = parts[2];
            long count;
            try {
                count = Long.parseLong(parts[3]);
            } catch (NumberFormatException e) {
                return; // invalid count
            }
            // Emit marginal key: (p,slot,*) with the same count
            outKey.set("PS\t" + p + "\t" +slot);
            outValue.set(count);
            context.write(outKey, outValue);
            // Emit marginal key: (*,slot,w) with the same count
            outKey.set("SW\t" + slot + "\t" +w);
            outValue.set(count);
            context.write(outKey, outValue);
            //Global marginal key: (*,slot,*) with the same count
            outKey.set("S\t" + slot);
            outValue.set(count);
            context.write(outKey, outValue);
        }
    }


    public static class ReducerClass extends Reducer<Text, LongWritable, Text, LongWritable> {

        private final LongWritable out = new LongWritable();

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable v : values){
                sum+=v.get();
            }
            out.set(sum);
            context.write(key, out);
            
        }
    }


    public static class CombinerClass extends Reducer<Text, LongWritable, Text, LongWritable> {
        private final LongWritable out = new LongWritable();

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable v : values){
                sum+=v.get();
            }
            out.set(sum);
            context.write(key, out);        
            
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("invalid input");
            System.exit(1);
        }

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Job2A - Marginals PS/SW and S");
        job.setJarByClass(Job2A_Marginals.class);

        job.setMapperClass(MapperClass.class);
        job.setCombinerClass(CombinerClass.class);
        job.setReducerClass(ReducerClass.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}