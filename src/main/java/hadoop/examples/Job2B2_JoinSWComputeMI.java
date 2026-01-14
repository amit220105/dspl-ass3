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
 * Job2B2: Reduce-side join between:
 *  - Job2B1 output: p \t slot \t w \t c \t ps
 *  - Job2A SW lines: SW \t slot \t w \t sw
 *
 * Also uses S(slot) loaded from Job2A in memory (only 2 keys: X/Y).
 *
 * Output:
 *  p \t slot \t w \t mi   (PMI+ only)
 */
public class Job2B2_JoinSWComputeMI {

    public static class CPSMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text outK = new Text();
        private final Text outV = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
            // input line format from Job2B1: key=p, value=slot \t w \t c \t ps
            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] parts = line.split("\t");
            // We expect: p \t slot \t w \t c \t ps  (because TextOutputFormat writes key\tvalue)
            if (parts.length != 5) return;

            String p    = parts[0];
            String slot = parts[1];
            String w    = parts[2];
            String c    = parts[3];
            String ps   = parts[4];

            // join key = slot \t w
            outK.set(slot + "\t" + w);
            outV.set("CPS\t" + p + "\t" + c + "\t" + ps);
            ctx.write(outK, outV);
        }
    }

    public static class SWMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text outK = new Text();
        private final Text outV = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] p = line.split("\t");
            if (p.length != 4) return;
            if (!"SW".equals(p[0])) return;

            String slot = p[1];
            String w    = p[2];
            String sw   = p[3];

            outK.set(slot + "\t" + w);
            outV.set("SW\t" + sw);
            ctx.write(outK, outV);
        }
    }

    public static class JoinAndComputeReducer extends Reducer<Text, Text, Text, Text> {

        private final java.util.Map<String, Long> sMap = new java.util.HashMap<>();

        @Override
        protected void setup(Context ctx) throws IOException, InterruptedException {
            // Load S(slot) from Job2A output by scanning reducer input? We receive job2AOut path via conf.
            // We'll scan Job2A in setup, but only keep S lines => tiny (X/Y).
            Configuration conf = ctx.getConfiguration();
            String job2aOut = conf.get("job2AOutputPath");
            if (job2aOut == null) throw new IOException("job2AOutputPath not set");

            org.apache.hadoop.fs.Path p = new org.apache.hadoop.fs.Path(job2aOut);
            org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(p.toUri(), conf);

            for (org.apache.hadoop.fs.FileStatus st : fs.listStatus(p)) {
                if (!st.isFile()) continue;
                String name = st.getPath().getName();
                if (!name.startsWith("part-")) continue;

                try (org.apache.hadoop.fs.FSDataInputStream in = fs.open(st.getPath());
                     java.io.BufferedReader br = new java.io.BufferedReader(new java.io.InputStreamReader(in))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        line = line.trim();
                        if (line.isEmpty()) continue;
                        String[] parts = line.split("\t");
                        if (parts.length == 3 && "S".equals(parts[0])) {
                            String slot = parts[1];
                            long s = Long.parseLong(parts[2]);
                            if (s > 0) sMap.put(slot, s);
                        }
                    }
                }
            }
            if (sMap.isEmpty()) throw new IOException("Failed to load S(slot) from Job2A output");
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {
            String swVal = null;
            java.util.List<String> cps = new java.util.ArrayList<>();

            for (Text t : values) {
                String s = t.toString();
                if (s.startsWith("SW\t")) swVal = s.split("\t", 2)[1];
                else if (s.startsWith("CPS\t")) cps.add(s);
            }
            if (swVal == null) return;

            String[] kw = key.toString().split("\t", 2); // slot \t w
            if (kw.length != 2) return;
            String slot = kw[0];
            String w    = kw[1];

            Long S = sMap.get(slot);
            if (S == null || S <= 0) return;

            long sw = Long.parseLong(swVal);
            if (sw <= 0) return;

            for (String rec : cps) {
                // CPS \t p \t c \t ps
                String[] p = rec.split("\t");
                if (p.length != 4) continue;

                String path = p[1];
                long c  = Long.parseLong(p[2]);
                long ps = Long.parseLong(p[3]);

                if (c <= 0 || ps <= 0) continue;

                double mi = Math.log((double)c * (double)S / ((double)ps * (double)sw));
                if (mi <= 0.0) continue; // PMI+

                ctx.write(new Text(path + "\t" + slot + "\t" + w), new Text(Double.toString(mi)));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        int off = 0;
        if (args.length >= 4 && args[0].contains(".") && !args[0].startsWith("s3://")) off = 1;
        if (args.length < off + 3) {
            System.err.println("Usage: Job2B2_JoinSWComputeMI <job2b1Out> <job2aOut> <job2bOut>");
            System.exit(1);
        }

        String job2b1Out = args[off];
        String job2aOut  = args[off + 1];
        String out       = args[off + 2];

        Configuration conf = new Configuration();
        conf.set("job2AOutputPath", job2aOut);

        Job job = Job.getInstance(conf, "Job2B2 - Join SW and compute MI (PMI+)");
        job.setJarByClass(Job2B2_JoinSWComputeMI.class);

        MultipleInputs.addInputPath(job, new Path(job2b1Out), TextInputFormat.class, CPSMapper.class);
        MultipleInputs.addInputPath(job, new Path(job2aOut),  TextInputFormat.class, SWMapper.class);

        job.setReducerClass(JoinAndComputeReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(out));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
