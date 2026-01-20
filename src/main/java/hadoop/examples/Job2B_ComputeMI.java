package hadoop.examples;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Locale;
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

public class Job2B_ComputeMI {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {

        private final Map<String, Long> psMap = new HashMap<>(); // key: p \t slot
        private final Map<String, Long> swMap = new HashMap<>(); // key: slot \t w
        private final Map<String, Long> sMap  = new HashMap<>(); // key: slot

        private final Text outKey = new Text();
        private final Text outValue = new Text();

        @Override
        protected void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            String job2APathStr = conf.get("job2AOutputPath");
            if (job2APathStr == null || job2APathStr.isEmpty()) {
                throw new IOException("Job2A output path not set in configuration (job2AOutputPath)");
            }

            Path job2APath = new Path(job2APathStr);
            FileSystem fs = job2APath.getFileSystem(conf);

            for (FileStatus status : fs.listStatus(job2APath)) {
                String fileName = status.getPath().getName();

                // Only read part-* files
                if (!fileName.startsWith("part-")) continue;

                try (FSDataInputStream in = fs.open(status.getPath());
                     BufferedReader br = new BufferedReader(new InputStreamReader(in))) {

                    String line;
                    while ((line = br.readLine()) != null) {
                        line = line.trim();
                        if (line.isEmpty()) continue;

                        String[] parts = line.split("\t");
                        if (parts.length < 3) continue;

                        String tag = parts[0];

                        if ("PS".equals(tag)) {
                            // PS \t p \t slot \t ps
                            if (parts.length != 4) continue;
                            long ps = parseLong(parts[3]);
                            if (ps > 0) psMap.put(parts[1] + "\t" + parts[2], ps);

                        } else if ("SW".equals(tag)) {
                            // SW \t slot \t w \t sw
                            if (parts.length != 4) continue;
                            long sw = parseLong(parts[3]);
                            if (sw > 0) swMap.put(parts[1] + "\t" + parts[2], sw);

                        } else if ("S".equals(tag)) {
                            // S \t slot \t s
                            if (parts.length != 3) continue;
                            long s = parseLong(parts[2]);
                            if (s > 0) sMap.put(parts[1], s);
                        }
                    }
                }
            }

            if (psMap.isEmpty() || swMap.isEmpty() || sMap.isEmpty()) {
                throw new IOException("Failed to load marginals from Job2A output (PS/SW/S empty)");
            }
        }

        private long parseLong(String s) {
            try { return Long.parseLong(s); }
            catch (NumberFormatException e) { return -1L; }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;

            // Job1: p \t slot \t w \t c
            String[] parts = line.split("\t");
            if (parts.length != 4) return;

            String p = parts[0];
            String slot = parts[1];
            String w = parts[2];

            long c;
            try {
                c = Long.parseLong(parts[3]);
            } catch (NumberFormatException e) {
                return;
            }

            Long ps = psMap.get(p + "\t" + slot);
            Long sw = swMap.get(slot + "\t" + w);
            Long s  = sMap.get(slot);

            if (ps == null || sw == null || s == null) return;
            if (c <= 0 || ps <= 0 || sw <= 0 || s <= 0) return;

            // Article MI (slot-conditioned):
            // mi = log( (|p,slot,w| * |*,slot,*|) / (|p,slot,*| * |*,slot,w|) )
            double mi = Math.log(((double) c * (double) s) / ((double) ps * (double) sw));

            outKey.set(p + "\t" + slot + "\t" + w);
            outValue.set(String.format(Locale.ROOT, "%.10f", mi));
            context.write(outKey, outValue);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: Job2B_ComputeMI <job1Out> <job2AOutDir> <job2BOut>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        conf.set("job2AOutputPath", args[1]);

        Job job = Job.getInstance(conf, "Job2B - Compute MI from Job2A marginals and Job1 counts");
        job.setJarByClass(Job2B_ComputeMI.class);

        job.setMapperClass(MapperClass.class);
        job.setNumReduceTasks(0); // map-only

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
