package hadoop.examples;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Input (Job3B2):
 *  slot \t p1 \t p2 \t numerator
 *
 * Needs slot sums file (Job3B0):
 *  p \t slot \t sumMI
 *
 * Output:
 *  p1 \t p2 \t finalSim
 *
 * Implementation: map-only, but only loads sums (NOT full vectors).
 */
public class Job3B3_FinalSimilarity {

    public static class M extends Mapper<LongWritable, Text, Text, Text> {
        private final Map<String, Double> sumMap = new HashMap<>();

        @Override
        protected void setup(Context ctx) throws IOException, InterruptedException {
            Configuration conf = ctx.getConfiguration();
            String sumsPathStr = conf.get("slotSumsPath");
            if (sumsPathStr == null) throw new IOException("slotSumsPath not set");

            Path sumsPath = new Path(sumsPathStr);
            FileSystem fs = FileSystem.get(sumsPath.toUri(), conf);

            for (FileStatus st : fs.listStatus(sumsPath)) {
                if (!st.isFile()) continue;
                String name = st.getPath().getName();
                if (!name.startsWith("part-")) continue;

                try (FSDataInputStream in = fs.open(st.getPath());
                     BufferedReader br = new BufferedReader(new InputStreamReader(in))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        line = line.trim();
                        if (line.isEmpty()) continue;
                        String[] p = line.split("\t");
                        if (p.length != 3) continue;
                        String key = p[0] + "\t" + p[1]; // p \t slot
                        double sum = Double.parseDouble(p[2]);
                        sumMap.put(key, sum);
                    }
                }
            }
            if (sumMap.isEmpty()) throw new IOException("Failed to load slot sums");
        }

        @Override
        protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;
            String[] p = line.split("\t");
            if (p.length != 4) return;

            String slot = p[0];
            String p1   = p[1];
            String p2   = p[2];
            double num;
            try { num = Double.parseDouble(p[3]); }
            catch (Exception e) { return; }

            Double s1 = sumMap.get(p1 + "\t" + slot);
            Double s2 = sumMap.get(p2 + "\t" + slot);
            if (s1 == null || s2 == null) return;

            double denom = s1 + s2;
            if (denom <= 0) return;

            double slotSim = num / denom;

            // Emit by pair so we can combine X and Y later:
            // key: p1 \t p2
            // val: slot \t slotSim
            ctx.write(new Text(p1 + "\t" + p2), new Text(slot + "\t" + Double.toString(slotSim)));
        }
    }

    public static void main(String[] args) throws Exception {
        int off = 0;
        if (args.length >= 4 && args[0].contains(".") && !args[0].startsWith("s3://")) off = 1;
        if (args.length < off + 3) {
            System.err.println("Usage: Job3B3_FinalSimilarity <job3b2Out> <job3b0SumsOut> <job3b3Out>");
            System.exit(1);
        }

        String in       = args[off];
        String sumsPath = args[off + 1];
        String out      = args[off + 2];

        Configuration conf = new Configuration();
        conf.set("slotSumsPath", sumsPath);

        Job job = Job.getInstance(conf, "Job3B3 - Compute slot similarities per pair");
        job.setJarByClass(Job3B3_FinalSimilarity.class);

        job.setMapperClass(M.class);
        job.setNumReduceTasks(0);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(in));
        FileOutputFormat.setOutputPath(job, new Path(out));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
