package hadoop.examples;

import java.io.*;
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

public class Job5_FinalSimilarity {

    // denom key: p \t slot  -> denom
    private static final Map<String, Double> denom = new HashMap<>();

    private static void loadDenomsFromCache(URI[] cacheFiles) throws IOException {
        if (!denom.isEmpty()) return;

        if (cacheFiles == null || cacheFiles.length == 0) {
            throw new IOException("No cache files for denominators");
        }

        for (URI uri : cacheFiles) {
            File f = new File(uri.getPath());
            String name = f.getName();
            // we may cache denoms + test files; skip test files
            String low = name.toLowerCase(Locale.ROOT);
            if (low.contains("positive") || low.contains("negative")) continue;

            try (BufferedReader br = new BufferedReader(new FileReader(f))) {
                String line;
                while ((line = br.readLine()) != null) {
                    line = line.trim();
                    if (line.isEmpty()) continue;

                    // Job3 output: "p \t slot \t denom"
                    String[] parts = line.split("\t");
                    if (parts.length != 3) continue;

                    String p = parts[0];
                    String slot = parts[1];
                    double d;
                    try { d = Double.parseDouble(parts[2]); }
                    catch (NumberFormatException e) { continue; }

                    denom.put(p + "\t" + slot, d);
                }
            }
        }

        if (denom.isEmpty()) {
            throw new IOException("Loaded 0 denominators from cache");
        }
    }

    // Mapper: pass through numer contributions
    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
        private final Text outKey = new Text();   // p1 \t p2
        private final Text outVal = new Text();   // slot \t contrib

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // load test pairs too (for labeling)
            TestPairsCache.loadFromCacheFiles(context.getCacheFiles());
            loadDenomsFromCache(context.getCacheFiles());
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // input is: key="p1\tp2\tslot" value="contrib"
            String line = value.toString().trim();
            if (line.isEmpty()) return;

            // Because TextOutputFormat writes "key\tvalue"
            // reconstruct:
            int tab = line.lastIndexOf('\t');
            if (tab < 0) return;

            String left = line.substring(0, tab);
            String contribStr = line.substring(tab + 1).trim();

            String[] kparts = left.split("\t");
            if (kparts.length != 3) return;

            String p1 = kparts[0];
            String p2 = kparts[1];
            String slot = kparts[2];

            double contrib;
            try { contrib = Double.parseDouble(contribStr); }
            catch (NumberFormatException e) { return; }

            outKey.set(p1 + "\t" + p2);
            outVal.set(slot + "\t" + contrib);
            context.write(outKey, outVal);
        }
    }

    // Reducer: aggregate contributions per slot, compute simX/simY and final S
    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            TestPairsCache.loadFromCacheFiles(context.getCacheFiles());
            loadDenomsFromCache(context.getCacheFiles());
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            String[] pp = key.toString().split("\t", 2);
            if (pp.length != 2) return;
            String p1 = pp[0];
            String p2 = pp[1];

            // Sum numerator per slot
            double numX = 0.0, numY = 0.0;
            for (Text tv : values) {
                String[] parts = tv.toString().split("\t");
                if (parts.length != 2) continue;
                String slot = parts[0];
                double c;
                try { c = Double.parseDouble(parts[1]); }
                catch (NumberFormatException e) { continue; }

                if ("X".equals(slot)) numX += c;
                else if ("Y".equals(slot)) numY += c;
            }

            Double d1X = denom.get(p1 + "\tX");
            Double d2X = denom.get(p2 + "\tX");
            Double d1Y = denom.get(p1 + "\tY");
            Double d2Y = denom.get(p2 + "\tY");

            if (d1X == null || d2X == null || d1Y == null || d2Y == null) return;

            double simX = (numX > 0.0) ? (numX / (d1X + d2X)) : 0.0;
            double simY = (numY > 0.0) ? (numY / (d1Y + d2Y)) : 0.0;

            double S = (simX > 0.0 && simY > 0.0) ? Math.sqrt(simX * simY) : 0.0;

            // label from test set
            String label = TestPairsCache.labelFor(p1, p2);

            // Output: p1 \t p2   label \t simX \t simY \t S
            context.write(new Text(p1 + "\t" + p2),
                          new Text(label + "\t" + simX + "\t" + simY + "\t" + S));
        }
    }

    public static void main(String[] args) throws Exception {
        // args:
        // <job4NumerInput> <job3DenomsPartFileS3> <positivePredsS3> <negativePredsS3> <outFinal>
        if (args.length != 5) {
            System.err.println("Usage: Job5_FinalSimilarity <numerIn> <denomsPartFile> <posPreds> <negPreds> <out>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "ASS3-Job5-FinalSimilarity");
        job.setJarByClass(Job5_FinalSimilarity.class);

        // cache: denoms + test sets
        job.addCacheFile(new URI(args[1])); // Job3 denom part file
        job.addCacheFile(new URI(args[2])); // positive
        job.addCacheFile(new URI(args[3])); // negative

        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[4]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}