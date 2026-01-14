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
 * Job3B4_TestSetSimilarity
 *
 * Inputs:
 *  (A) Job3B3 output (TextOutputFormat):
 *      p1 \t p2 \t slot \t slotSim
 *  (B) Test-set file:
 *      path1 \t path2 \t label   (label 0/1)
 *
 * Output:
 *      path1 \t path2 \t finalSim \t label
 * where finalSim = sqrt(slotSimX * slotSimY), missing slot => sim=0
 */
public class Job3B4_TestSetSimilarity {

    // Canonicalize pair key so joins work even if test-set order differs.
    private static String canonKey(String a, String b) {
        return (a.compareTo(b) <= 0) ? (a + "\t" + b) : (b + "\t" + a);
    }

    /** Reads Job3B3 output lines: p1 \t p2 \t slot \t slotSim */
    public static class SlotSimMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text outK = new Text();
        private final Text outV = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] p = line.split("\t");
            if (p.length != 4) return;

            String p1 = p[0];
            String p2 = p[1];
            String slot = p[2];
            String slotSim = p[3];

            // Expect slot X/Y, but be tolerant
            if (!"X".equals(slot) && !"Y".equals(slot)) return;

            outK.set(canonKey(p1, p2));
            outV.set("S\t" + slot + "\t" + slotSim);
            ctx.write(outK, outV);
        }
    }

    /** Reads test-set lines: path1 \t path2 \t label */
    public static class TestSetMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text outK = new Text();
        private final Text outV = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] p = line.split("\t");
            if (p.length < 2) return;

            String a = p[0];
            String b = p[1];
            String label = (p.length >= 3) ? p[2] : "0";

            // keep original order for output
            outK.set(canonKey(a, b));
            outV.set("T\t" + a + "\t" + b + "\t" + label);
            ctx.write(outK, outV);
        }
    }

    public static class JoinReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {
            Double sx = null, sy = null;

            // test-set may theoretically contain duplicates => output each
            java.util.List<String[]> testRecs = new java.util.ArrayList<>();

            for (Text t : values) {
                String s = t.toString();
                if (s.startsWith("S\t")) {
                    // S \t slot \t slotSim
                    String[] p = s.split("\t");
                    if (p.length != 3) continue;
                    double sim;
                    try { sim = Double.parseDouble(p[2]); }
                    catch (Exception e) { continue; }

                    if ("X".equals(p[1])) sx = sim;
                    else if ("Y".equals(p[1])) sy = sim;

                } else if (s.startsWith("T\t")) {
                    // T \t a \t b \t label
                    String[] p = s.split("\t", 4);
                    if (p.length != 4) continue;
                    // p[1]=a, p[2]=b, p[3]=label
                    testRecs.add(new String[]{p[1], p[2], p[3]});
                }
            }

            if (testRecs.isEmpty()) return; // not in test-set => ignore

            double finalSim = 0.0;
            if (sx != null && sy != null) {
                finalSim = Math.sqrt(sx * sy);
                if (Double.isNaN(finalSim) || finalSim < 0.0) finalSim = 0.0;
            }

            for (String[] r : testRecs) {
                String a = r[0], b = r[1], label = r[2];
                ctx.write(new Text(a), new Text(b + "\t" + finalSim + "\t" + label));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        System.err.println("Job3B4_TestSetSimilarity args=" + java.util.Arrays.toString(args));

        int off = 0;
        if (args.length >= 4 && args[0].contains(".") && !args[0].startsWith("s3://")) off = 1;

        if (args.length < off + 3) {
            System.err.println("Usage: Job3B4_TestSetSimilarity <job3b3Out> <testSetS3> <job3b4Out>");
            System.exit(1);
        }

        String job3b3Out = args[off];
        String testSet   = args[off + 1];
        String out       = args[off + 2];

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Job3B4 - TestSet Similarity (join + finalSim)");
        job.setJarByClass(Job3B4_TestSetSimilarity.class);

        MultipleInputs.addInputPath(job, new Path(job3b3Out), TextInputFormat.class, SlotSimMapper.class);
        MultipleInputs.addInputPath(job, new Path(testSet),   TextInputFormat.class, TestSetMapper.class);

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
