package hadoop.examples;


import java.io.IOException;
import java.util.List;

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

import hadoop.packages.BiarcsExtractor;
import hadoop.packages.BiarcsParser;


public class Job1TripleCounts {


    public static class MapperClass extends Mapper<LongWritable, Text, Text, LongWritable> {
            private final Text outKey = new Text();
            private final LongWritable outValue = new LongWritable();
            BiarcsExtractor extractor = new BiarcsExtractor();


        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.getCounter("DBG", "IN").increment(1);

            String raw = value.toString();
            if (raw.trim().isEmpty()) {
                context.getCounter("DBG", "EMPTY").increment(1);
                return;
            }

            BiarcsParser.Record record;
            try {
                record = BiarcsParser.parseLine(raw);   // IMPORTANT: parse raw, not trimmed
            } catch (Exception e) {
                context.getCounter("DBG", "PARSE_EX").increment(1);
                return;
            }
            if (record == null) {
                context.getCounter("DBG", "PARSE_NULL").increment(1);
                return;
            }
            context.getCounter("DBG", "PARSE_OK").increment(1);
            List<BiarcsExtractor.Event> events = extractor.extract(record);
            if (events == null || events.isEmpty()) {
                context.getCounter("DBG", "NO_EVENTS").increment(1);
                return;
            }
            context.getCounter("DBG", "EVENTS").increment(events.size());
            for (BiarcsExtractor.Event event : events) {
                outKey.set(event.predicateKey + "\t" + event.slot + "\t" + event.filler);
                outValue.set(event.count);
                context.write(outKey, outValue);
                context.getCounter("DBG", "EMITTED").increment(1);
            }
        }
    }

    /** Reducer: sum counts for each (p,slot,w) */
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

    // Optional combiner: SAME as reducer (safe because sum is associative/commutative)
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
    System.err.println("Job1TripleCounts args=" + java.util.Arrays.toString(args));

    int off = 0;
    if (args.length >= 3 && args[0].contains(".") && !args[0].startsWith("s3://")) {
        off = 1; // skip mainClass being injected
    }

    if (args.length < off + 2) {
        System.err.println("invalid input. args=" + java.util.Arrays.toString(args));
        System.exit(1);
    }

    String in = args[off];
    String out = args[off + 1];

    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Job1 - Triple Counts |p,slot,w|");
    job.getConfiguration().set("mapreduce.output.textoutputformat.separator", "\t");
    job.getConfiguration().set("mapred.textoutputformat.separator", "\t"); // for older Hadoop
    job.setJarByClass(Job1TripleCounts.class);

    job.setMapperClass(MapperClass.class);
    job.setCombinerClass(CombinerClass.class);
    job.setReducerClass(ReducerClass.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(LongWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    FileInputFormat.addInputPath(job, new Path(in));
    FileOutputFormat.setOutputPath(job, new Path(out));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
}
}