package hadoop.examples;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
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

public class Job4_TestSetSimilarityDirect {

    private static String canonKey(String a, String b) {
        return (a.compareTo(b) <= 0) ? (a + "\t" + b) : (b + "\t" + a);
    }

    /** Robustly open a cache URI: try localized symlink (fragment), else open via FileSystem (S3). */
    private static BufferedReader openCacheUri(Configuration conf, URI u) throws IOException {
        // 1) If Hadoop created a local symlink, it will have the fragment name
        String localName = u.getFragment();
        if (localName == null || localName.isEmpty()) {
            // fallback: just the filename
            localName = new Path(u.getPath()).getName();
        }

        File f = new File(localName);
        if (f.exists()) {
            return new BufferedReader(new InputStreamReader(new FileInputStream(f), StandardCharsets.UTF_8));
        }

        // 2) Fallback: open the *real* URI (strip fragment) using FileSystem
        try {
            URI noFrag = new URI(u.getScheme(), u.getAuthority(), u.getPath(), null, null);
            Path p = new Path(noFrag.toString());
            FileSystem fs = p.getFileSystem(conf);
            FSDataInputStream in = fs.open(p);
            return new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
        } catch (URISyntaxException e) {
            throw new IOException("Bad cache URI: " + u, e);
        }
    }

    private static class PairRec {
        final String a, b, label;
        PairRec(String a, String b, String label) { this.a = a; this.b = b; this.label = label; }
    }

    private static class PathData {
        final Map<String, Double> x = new HashMap<>();
        final Map<String, Double> y = new HashMap<>();
        double sumX = 0.0;
        double sumY = 0.0;
    }

    public static class M extends Mapper<LongWritable, Text, Text, Text> {

        private final Map<String, List<String>> pairKeysByPath = new HashMap<>();
        private final Text outK = new Text();
        private final Text outV = new Text();

        @Override
        protected void setup(Context ctx) throws IOException {
            URI[] cacheFiles = ctx.getCacheFiles();
            if (cacheFiles == null || cacheFiles.length == 0) {
                throw new IOException("Missing test-set cache file (job.addCacheFile not applied?)");
            }

            Configuration conf = ctx.getConfiguration();

            for (URI u : cacheFiles) {
                try (BufferedReader br = openCacheUri(conf, u)) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        line = line.trim();
                        if (line.isEmpty()) continue;

                        String[] p = line.split("\t");
                        if (p.length < 2) continue;

                        String a = p[0];
                        String b = p[1];
                        String pairKey = canonKey(a, b);

                        pairKeysByPath.computeIfAbsent(a, k -> new ArrayList<>()).add(pairKey);
                        pairKeysByPath.computeIfAbsent(b, k -> new ArrayList<>()).add(pairKey);
                    }
                }
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;

            // MI input line:   p \t slot \t w \t mi   (4 cols)
            // SUMS input line: p \t slot \t sumMI     (3 cols)
            String[] p = line.split("\t");
            if (p.length != 3 && p.length != 4) return;

            String path = p[0];
            List<String> pairKeys = pairKeysByPath.get(path);
            if (pairKeys == null || pairKeys.isEmpty()) return;

            if (p.length == 4) {
                String slot = p[1];
                String w    = p[2];
                String mi   = p[3];

                for (String pairKey : pairKeys) {
                    outK.set(pairKey);
                    outV.set("F\t" + path + "\t" + slot + "\t" + w + "\t" + mi);
                    ctx.write(outK, outV);
                }
            } else {
                String slot = p[1];
                String sum  = p[2];

                for (String pairKey : pairKeys) {
                    outK.set(pairKey);
                    outV.set("S\t" + path + "\t" + slot + "\t" + sum);
                    ctx.write(outK, outV);
                }
            }
        }
    }

    public static class R extends Reducer<Text, Text, Text, Text> {

        private final Map<String, List<PairRec>> pairRecs = new HashMap<>();

        @Override
        protected void setup(Context ctx) throws IOException {
            URI[] cacheFiles = ctx.getCacheFiles();
            if (cacheFiles == null || cacheFiles.length == 0) {
                throw new IOException("Missing test-set cache file (job.addCacheFile not applied?)");
            }

            Configuration conf = ctx.getConfiguration();

            for (URI u : cacheFiles) {
                try (BufferedReader br = openCacheUri(conf, u)) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        line = line.trim();
                        if (line.isEmpty()) continue;

                        String[] p = line.split("\t");
                        if (p.length < 2) continue;

                        String a = p[0];
                        String b = p[1];
                        String label = (p.length >= 3) ? p[2] : "0";

                        String pairKey = canonKey(a, b);
                        pairRecs.computeIfAbsent(pairKey, k -> new ArrayList<>()).add(new PairRec(a, b, label));
                    }
                }
            }
        }

        private static double parseDoubleSafe(String s) {
            try { return Double.parseDouble(s); } catch (Exception e) { return 0.0; }
        }

        private static double slotSim(Map<String, Double> m1, Map<String, Double> m2, double sum1, double sum2) {
            double denom = sum1 + sum2;
            if (denom <= 0.0) return 0.0;
            if (m1.isEmpty() || m2.isEmpty()) return 0.0;

            Map<String, Double> small = (m1.size() <= m2.size()) ? m1 : m2;
            Map<String, Double> big   = (small == m1) ? m2 : m1;

            double num = 0.0;
            for (Map.Entry<String, Double> e : small.entrySet()) {
                Double v = big.get(e.getKey());
                if (v != null) num += e.getValue() + v;
            }

            double sim = num / denom;
            if (Double.isNaN(sim) || sim < 0.0) return 0.0;
            return sim;
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {
            String pairKey = key.toString();
            List<PairRec> recs = pairRecs.get(pairKey);
            if (recs == null || recs.isEmpty()) return;

            String[] kp = pairKey.split("\t", 2);
            if (kp.length != 2) return;

            String pMin = kp[0];
            String pMax = kp[1];

            PathData dMin = new PathData();
            PathData dMax = new PathData();

            for (Text t : values) {
                String s = t.toString();
                String[] p = s.split("\t");
                if (p.length == 0) continue;

                if ("S".equals(p[0])) {
                    // S \t path \t slot \t sum
                    if (p.length != 4) continue;
                    String path = p[1];
                    String slot = p[2];
                    double sum  = parseDoubleSafe(p[3]);

                    PathData d = path.equals(pMin) ? dMin : (path.equals(pMax) ? dMax : null);
                    if (d == null) continue;

                    if ("X".equals(slot)) d.sumX = sum;
                    else if ("Y".equals(slot)) d.sumY = sum;

                } else if ("F".equals(p[0])) {
                    // F \t path \t slot \t w \t mi
                    if (p.length != 5) continue;
                    String path = p[1];
                    String slot = p[2];
                    String w    = p[3];
                    double mi   = parseDoubleSafe(p[4]);

                    PathData d = path.equals(pMin) ? dMin : (path.equals(pMax) ? dMax : null);
                    if (d == null) continue;

                    if ("X".equals(slot)) d.x.put(w, mi);
                    else if ("Y".equals(slot)) d.y.put(w, mi);
                }
            }

            double simX = slotSim(dMin.x, dMax.x, dMin.sumX, dMax.sumX);
            double simY = slotSim(dMin.y, dMax.y, dMin.sumY, dMax.sumY);

            double finalSim = Math.sqrt(simX * simY);
            if (Double.isNaN(finalSim) || finalSim < 0.0) finalSim = 0.0;

            for (PairRec r : recs) {
                ctx.write(new Text(r.a), new Text(r.b + "\t" + Double.toString(finalSim) + "\t" + r.label));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        System.err.println("Job4_TestSetSimilarityDirect args=" + java.util.Arrays.toString(args));

        int off = 0;
        if (args.length >= 5 && args[0].contains(".") && !args[0].startsWith("s3://")) off = 1;

        if (args.length < off + 4) {
            System.err.println("Usage: Job4_TestSetSimilarityDirect <miPath> <sumsPath> <testSetS3> <out>");
            System.exit(1);
        }

        String miPath   = args[off];
        String sumsPath = args[off + 1];
        String testSet  = args[off + 2];
        String out      = args[off + 3];

        Job job = Job.getInstance(new Configuration(), "Job4 - Direct TestSet Similarity");
        job.setJarByClass(Job4_TestSetSimilarityDirect.class);

        // Strongly request symlinks (some EMR/Hadoop builds ignore this; we still have FS fallback)
        job.getConfiguration().setBoolean("mapreduce.job.cache.files.symlink.create", true);
        job.getConfiguration().setBoolean("mapred.create.symlink", true);

        // Cache testset; fragment becomes preferred local symlink name
        Path ts = new Path(testSet);
        job.addCacheFile(new URI(ts.toString() + "#testset.txt"));

        job.setMapperClass(M.class);
        job.setReducerClass(R.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(miPath));
        FileInputFormat.addInputPath(job, new Path(sumsPath));
        FileOutputFormat.setOutputPath(job, new Path(out));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
