package hadoop.examples;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Job2B1: Reduce-side join between:
 *  - Job1: p \t slot \t w \t c
 *  - Job2A: PS \t p \t slot \t ps
 *
 * Output:
 *  p \t slot \t w \t c \t ps
 */
public class Job2B1_JoinPS {

    public static class Job1Mapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text outK = new Text();
        private final Text outV = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;
            String[] p = line.split("\t");
            if (p.length != 4) return;

            String path = p[0];
            String slot = p[1];
            String w    = p[2];
            String c    = p[3];

            // join key = p \t slot
            outK.set(path + "\t" + slot);
            outV.set("C\t" + w + "\t" + c);
            ctx.write(outK, outV);
        }
    }

    public static class PSMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text outK = new Text();
        private final Text outV = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;
            String[] p = line.split("\t");
            if (p.length != 4) return;
            if (!"PS".equals(p[0])) return;

            String path = p[1];
            String slot = p[2];
            String ps   = p[3];

            outK.set(path + "\t" + slot);
            outV.set("PS\t" + ps);
            ctx.write(outK, outV);
        }
    }

    public static class JoinReducer extends Reducer<Text, Text, Text, Text> {
        private final Text outK = new Text();
        private final Text outV = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {
            String psVal = null;

            // store counts temporarily (usually few)
            java.util.List<String> cs = new java.util.ArrayList<>();

            for (Text t : values) {
                String s = t.toString();
                if (s.startsWith("PS\t")) {
                    psVal = s.split("\t", 2)[1];
                } else if (s.startsWith("C\t")) {
                    cs.add(s);
                }
            }
            if (psVal == null) return;

            String[] ks = key.toString().split("\t", 2);
            if (ks.length != 2) return;
            String path = ks[0];
            String slot = ks[1];

            for (String cRec : cs) {
                // C \t w \t c
                String[] p = cRec.split("\t");
                if (p.length != 3) continue;
                String w = p[1];
                String c = p[2];

                outK.set(path);
                outV.set(slot + "\t" + w + "\t" + c + "\t" + psVal);
                // output = p \t slot \t w \t c \t ps  (spread across key/value)
                ctx.write(outK, outV);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        int off = 0;
        if (args.length >= 4 && args[0].contains(".") && !args[0].startsWith("s3://")) off = 1;
        if (args.length < off + 3) {
            System.err.println("Usage: Job2B1_JoinPS <job1Out> <job2aOut> <job2b1Out>");
            System.exit(1);
        }

        String job1Out  = args[off];
        String job2aOut = args[off + 1];
        String out      = args[off + 2];

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Job2B1 - Join Job1 counts with PS");
        job.setJarByClass(Job2B1_JoinPS.class);

        MultipleInputs.addInputPath(job, new Path(job1Out),  TextInputFormat.class, Job1Mapper.class);
        MultipleInputs.addInputPath(job, new Path(job2aOut), TextInputFormat.class, PSMapper.class);

        job.setReducerClass(JoinReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(out));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
