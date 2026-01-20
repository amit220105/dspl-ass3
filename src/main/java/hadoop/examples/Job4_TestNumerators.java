package hadoop.examples;

import java.io.IOException;
import java.net.URI;
import java.util.*;

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

public class Job4_TestNumerators {

    // Mapper: (p,slot,w,mi) -> key=(slot,w) value=(p,mi)
    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
        private final Text outKey = new Text();
        private final Text outVal = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] parts = line.split("\t");
            if (parts.length != 4) return;

            String p = parts[0];
            String slot = parts[1];
            String w = parts[2];

            double mi;
            try { mi = Double.parseDouble(parts[3]); }
            catch (NumberFormatException e) { return; }

            if (mi <= 0.0) return;

            outKey.set(slot + "\t" + w);
            outVal.set(p + "\t" + mi);
            context.write(outKey, outVal);
        }
    }

    // Reducer: for each (slot,w) see which paths are present and emit only test pairs
    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            TestPairsCache.loadFromCacheFiles(cacheFiles);
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            // key = slot \t w
            String[] kw = key.toString().split("\t", 2);
            if (kw.length != 2) return;
            String slot = kw[0];

            // Collect mi per predicate for this feature
            Map<String, Double> miByP = new HashMap<>();
            for (Text tv : values) {
                String[] parts = tv.toString().split("\t");
                if (parts.length != 2) continue;
                String p = parts[0];
                double mi;
                try { mi = Double.parseDouble(parts[1]); }
                catch (NumberFormatException e) { continue; }
                miByP.put(p, mi); // if duplicates exist, last wins; ideally there aren't
            }

            if (miByP.size() < 2) return;

            // For each p in this feature, only pair it with partners in test set that also exist here
            for (Map.Entry<String, Double> e : miByP.entrySet()) {
                String p = e.getKey();
                Double miP = e.getValue();

                Set<String> partners = TestPairsCache.partners.get(p);
                if (partners == null || partners.isEmpty()) continue;

                for (String q : partners) {
                    Double miQ = miByP.get(q);
                    if (miQ == null) continue; // q doesn't have this feature

                    // emit canonical order to avoid duplicates
                    String p1 = (p.compareTo(q) <= 0) ? p : q;
                    String p2 = (p.compareTo(q) <= 0) ? q : p;

                    // numerator contribution for this (slot,w)
                    double contrib = miP + miQ;

                    // output: key=(p1 \t p2 \t slot) value=contrib
                    context.write(new Text(p1 + "\t" + p2 + "\t" + slot),
                                  new Text(Double.toString(contrib)));
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        // args: <miInput> <posPredsS3> <negPredsS3> <outNumer>
        if (args.length != 4) {
            System.err.println("Usage: Job4_TestNumerators <miInput> <positivePreds> <negativePreds> <outNumer>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "ASS3-Job4-TestNumerators");
        job.setJarByClass(Job4_TestNumerators.class);

        job.addCacheFile(new URI(args[1])); // positive
        job.addCacheFile(new URI(args[2])); // negative

        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[3]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}