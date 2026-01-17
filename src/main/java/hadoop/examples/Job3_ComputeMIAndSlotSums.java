package hadoop.examples;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Job3_ComputeMIAndSlotSums
 *
 * Inputs:
 *  (A) Job1 output:   p \t slot \t w \t c
 *  (B) Job2A output:  PS \t p \t slot \t ps
 *                    SW \t slot \t w \t sw
 *                     S \t slot \t s
 *
 * Output (under <outBase>/):
 *  - mi/   : p \t slot \t w \t mi   (PMI+ only)
 *  - sums/ : p \t slot \t sumMI     (sum of PMI+ per (p,slot))
 */
public class Job3_ComputeMIAndSlotSums {

    private static final String OUT_MI   = "mi";
    private static final String OUT_SUMS = "sums";

    public static class M extends Mapper<LongWritable, Text, Text, Text> {

        private final Map<String, Long> psMap = new HashMap<>(); // key: p\tslot
        private final Map<String, Long> swMap = new HashMap<>(); // key: slot\tw
        private final Map<String, Long> sMap  = new HashMap<>(); // key: slot

        private final Text outK = new Text();
        private final Text outV = new Text();

        @Override
        protected void setup(Context ctx) throws IOException {
            URI[] cacheFiles = ctx.getCacheFiles();
            if (cacheFiles == null || cacheFiles.length == 0) {
                throw new IOException("No cache files found for Job2A marginals");
            }

            Configuration conf = ctx.getConfiguration();

            for (URI u : cacheFiles) {
                // Prefer the localized symlink name (fragment), fallback to FS open.
                String localName = u.getFragment();
                if (localName == null || localName.isEmpty()) {
                    localName = new Path(u.getPath()).getName();
                }

                BufferedReader br = null;

                try {
                    File f = new File(localName);
                    if (f.exists()) {
                        br = new BufferedReader(new InputStreamReader(new FileInputStream(f), StandardCharsets.UTF_8));
                    } else {
                        // Fallback: open via FileSystem (works even if cache didn't symlink)
                        Path p = new Path(u.getScheme(), u.getAuthority(), u.getPath()); // strip fragment
                        FileSystem fs = FileSystem.get(p.toUri(), conf);
                        FSDataInputStream in = fs.open(p);
                        br = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
                    }

                    String line;
                    while ((line = br.readLine()) != null) {
                        line = line.trim();
                        if (line.isEmpty()) continue;

                        String[] p = line.split("\t");
                        if (p.length == 4 && "PS".equals(p[0])) {
                            long ps = parseLong(p[3]);
                            if (ps > 0) psMap.put(p[1] + "\t" + p[2], ps);

                        } else if (p.length == 4 && "SW".equals(p[0])) {
                            long sw = parseLong(p[3]);
                            if (sw > 0) swMap.put(p[1] + "\t" + p[2], sw);

                        } else if (p.length == 3 && "S".equals(p[0])) {
                            long s = parseLong(p[2]);
                            if (s > 0) sMap.put(p[1], s);
                        }
                    }
                } finally {
                    if (br != null) br.close();
                }
            }

            if (psMap.isEmpty() || swMap.isEmpty() || sMap.isEmpty()) {
                throw new IOException("Failed loading marginals: PS=" + psMap.size() +
                        " SW=" + swMap.size() + " S=" + sMap.size());
            }
        }

        private long parseLong(String s) {
            try { return Long.parseLong(s); } catch (Exception e) { return -1L; }
        }

        @Override
        protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;

            // Job1 output: p \t slot \t w \t c
            String[] p = line.split("\t");
            if (p.length != 4) return;

            String path = p[0];
            String slot = p[1];
            String w    = p[2];

            long c;
            try { c = Long.parseLong(p[3]); }
            catch (Exception e) { return; }
            if (c <= 0) return;

            Long ps = psMap.get(path + "\t" + slot);
            Long sw = swMap.get(slot + "\t" + w);
            Long S  = sMap.get(slot);

            if (ps == null || sw == null || S == null) return;
            if (ps <= 0 || sw <= 0 || S <= 0) return;

            double mi = Math.log((double)c * (double)S / ((double)ps * (double)sw));
            if (mi <= 0.0 || Double.isNaN(mi) || Double.isInfinite(mi)) return; // PMI+

            // Group by (p,slot) so reducer can compute sumMI and also emit all MI lines
            outK.set(path + "\t" + slot);
            outV.set(w + "\t" + Double.toString(mi));
            ctx.write(outK, outV);
        }
    }

    public static class R extends Reducer<Text, Text, Text, Text> {

        private MultipleOutputs<Text, Text> mos;

        @Override
        protected void setup(Context ctx) {
            mos = new MultipleOutputs<>(ctx);
        }

        @Override
        protected void cleanup(Context ctx) throws IOException, InterruptedException {
            mos.close();
        }

        static class WM {
            final String w;
            final double mi;
            WM(String w, double mi) { this.w = w; this.mi = mi; }
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {
            // key: p \t slot
            String[] ks = key.toString().split("\t", 2);
            if (ks.length != 2) return;
            String path = ks[0];
            String slot = ks[1];

            List<WM> list = new ArrayList<>();
            double sumMI = 0.0;

            for (Text t : values) {
                String[] p = t.toString().split("\t", 2); // w \t mi
                if (p.length != 2) continue;
                double mi;
                try { mi = Double.parseDouble(p[1]); }
                catch (Exception e) { continue; }
                if (mi <= 0.0) continue;

                list.add(new WM(p[0], mi));
                sumMI += mi;
            }

            // sums/: p \t slot \t sumMI   (written as key=p, value=slot\tsumMI)
            mos.write(OUT_SUMS, new Text(path), new Text(slot + "\t" + Double.toString(sumMI)), OUT_SUMS + "/part");

            // mi/: p \t slot \t w \t mi  (written as key=p, value=slot\tw\tmi)
            for (WM wm : list) {
                mos.write(OUT_MI, new Text(path),
                        new Text(slot + "\t" + wm.w + "\t" + Double.toString(wm.mi)),
                        OUT_MI + "/part");
            }
        }
    }

    public static void main(String[] args) throws Exception {
        System.err.println("Job3_ComputeMIAndSlotSums args=" + java.util.Arrays.toString(args));

        int off = 0;
        if (args.length >= 4 && args[0].contains(".") && !args[0].startsWith("s3://")) off = 1;

        if (args.length < off + 3) {
            System.err.println("Usage: Job3_ComputeMIAndSlotSums <job1Out> <job2aOut> <outBase>");
            System.exit(1);
        }

        String job1Out  = args[off];
        String job2aOut = args[off + 1];
        String outBase  = args[off + 2];

        Configuration conf = new Configuration();
        // Ensure YARN creates symlinks for cache files when using "#name"
        conf.setBoolean("mapreduce.job.cache.files.symlink.create", true);

        Job job = Job.getInstance(conf, "Job3 - Compute MI (PMI+) + SlotSums");
        job.setJarByClass(Job3_ComputeMIAndSlotSums.class);

        // Add Job2A part files to Distributed Cache with "#localName" so we can open them locally
        Path margPath = new Path(job2aOut);
        FileSystem fs = FileSystem.get(margPath.toUri(), conf);

        for (FileStatus st : fs.listStatus(margPath)) {
            if (!st.isFile()) continue;
            String name = st.getPath().getName();
            if (!name.startsWith("part-")) continue;

            // Important: "#name" => localized symlink "name" in task working dir
            job.addCacheFile(new URI(st.getPath().toString() + "#" + name));
        }

        job.setMapperClass(M.class);
        job.setReducerClass(R.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        MultipleOutputs.addNamedOutput(job, OUT_MI, TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job, OUT_SUMS, TextOutputFormat.class, Text.class, Text.class);

        FileInputFormat.addInputPath(job, new Path(job1Out));
        FileOutputFormat.setOutputPath(job, new Path(outBase));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
