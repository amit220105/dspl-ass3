package hadoop.examples;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

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
 * Build pair contributions from inverted index.
 *
 * Input (Job3A):
 *  p \t slot \t w \t mi \t sumMI
 *
 * Map:
 *  key = slot \t w
 *  val = p \t mi
 *
 * Reduce:
 *  For each feature, keep top N paths by mi (to bound explosion),
 *  emit:
 *    slot \t p1 \t p2   -> (mi1 + mi2)
 */
public class Job3B1_FeaturePairContrib {

    static class PM {
        String p;
        double mi;
        PM(String p, double mi) { this.p = p; this.mi = mi; }
    }

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
            String w    = p[2];
            String mi   = p[3];

            k.set(slot + "\t" + w);
            v.set(path + "\t" + mi);
            ctx.write(k, v);
        }
    }

    public static class R extends Reducer<Text, Text, Text, Text> {
        private int maxPathsPerFeature;

        @Override
        protected void setup(Context ctx) throws IOException, InterruptedException {
            Configuration conf = ctx.getConfiguration();
            maxPathsPerFeature = conf.getInt("maxPathsPerFeature", 100); // tuneable
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {
            // keep top maxPathsPerFeature by MI
            PriorityQueue<PM> pq = new PriorityQueue<>(Comparator.comparingDouble(a -> a.mi)); // min-heap

            for (Text t : values) {
                String[] p = t.toString().split("\t");
                if (p.length != 2) continue;
                double mi;
                try { mi = Double.parseDouble(p[1]); }
                catch (Exception e) { continue; }

                pq.add(new PM(p[0], mi));
                if (pq.size() > maxPathsPerFeature) pq.poll();
            }

            if (pq.size() < 2) return;

            List<PM> list = new ArrayList<>(pq);
            list.sort((a,b) -> Double.compare(b.mi, a.mi)); // desc

            String[] kw = key.toString().split("\t", 2);
            if (kw.length != 2) return;
            String slot = kw[0];

            for (int i = 0; i < list.size(); i++) {
                for (int j = i + 1; j < list.size(); j++) {
                    PM a = list.get(i), b = list.get(j);
                    String p1 = a.p, p2 = b.p;
                    // normalize order
                    if (p1.compareTo(p2) > 0) { String tmp = p1; p1 = p2; p2 = tmp; }

                    double contrib = a.mi + b.mi;
                    ctx.write(new Text(slot + "\t" + p1 + "\t" + p2), new Text(Double.toString(contrib)));
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        int off = 0;
        if (args.length >= 3 && args[0].contains(".") && !args[0].startsWith("s3://")) off = 1;
        if (args.length < off + 2) {
            System.err.println("Usage: Job3B1_FeaturePairContrib <job3aOut> <job3b1Out> [maxPathsPerFeature]");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        if (args.length > off + 2) conf.setInt("maxPathsPerFeature", Integer.parseInt(args[off + 2]));

        Job job = Job.getInstance(conf, "Job3B1 - Feature -> Pair contributions");
        job.setJarByClass(Job3B1_FeaturePairContrib.class);

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
