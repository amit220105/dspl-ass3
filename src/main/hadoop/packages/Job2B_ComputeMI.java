package hadoop.examples;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

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


public class Job2B_ComputeMI {


    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
            private final Map<String, Long> psMap = new HashMap<>();
            private final Map<String, Long> swMap = new HashMap<>();
            private final Map<String, Long> sMap = new HashMap<>();
            private final Text outKey = new Text();
            private final Text outValue = new Text();


        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String job2APathStr = conf.get("job2AOutputPath");
            if (job2APathStr == null) {
                throw new IOException("Job2A output path not set in configuration");
            }
            Path job2APath = new Path(job2APathStr);
            FileSystem fs = FileSystem.get(conf);
            for (FileStatus status : fs.listStatus(job2APath)) {
                String fileName = status.getPath().getName();
                if (fileName.startsWith("part-")) continue; // skip hidden files
                try(FsDataInputStream in = fs.open(status.getPath());
                    BufferedReader br = new BufferedReader(new InputStreamReader(in))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        line = line.trim();
                        if (line.isEmpty()) continue;
                        String[] parts = line.split("\t");
                        if (parts.length < 3) continue; // invalid line
                        if (parts[0].equals("PS")) {
                            if (parts.length != 4) continue; // invalid line
                            long ps = Long.parseLong(parts[3]);
                            if (ps >0) psMap.put(parts[1] + "\t" + parts[2], ps);
                        } else if (parts[0].equals("SW")) {
                            if (parts.length != 4) continue; // invalid line
                            long sw = Long.parseLong(parts[3]);
                            if (sw >0) swMap.put(parts[1] + "\t" + parts[2], sw);
                        } else if (parts[0].equals("S")) {
                            if (parts.length != 3) continue; // invalid line
                            long s = Long.parseLong(parts[2]);
                            if (s >0) sMap.put(parts[1], s);
                        }
                    }
                }
            }
            if (psMap.isEmpty() || swMap.isEmpty() || sMap.isEmpty()) {
                throw new IOException("Failed to load marginals from Job2A output");
            }
        }

        private long parseLong(String s) {
            try { return Long.parseLong(s); }
            catch (NumberFormatException e) { return -1L; }
        }

        @Override
        protected void map(LongWritable key, org.w3c.dom.Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;
            String[] parts = line.split("\t");
            if (parts.length != 4) return; // invalid line
            // Job1: p \t slot \t w \t c
            String p = parts[0];
            String slot = parts[1];
            String w = parts[2];
            long count;
            try {
                count = Long.parseLong(parts[3]);
            } catch (NumberFormatException e) {
                return; // invalid count
            }
            Long ps = psMap.get(p + "\t" + slot);
            Long sw = swMap.get(slot + "\t" + w);
            Long s = sMap.get(slot);
            if (ps == null || sw == null || s == null) return; // missing marginals
            if (c <= 0 || ps <= 0 || sw <= 0 || s <= 0) return;
            double mi = Math.log((double)c * (double)s / ((double)ps * (double)sw));
            outKey.set(p + "\t" + slot + "\t" + w);
            outValue.set(""+mi);
            context.write(outKey, outValue);
        }    
    }



    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("invalid input");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        conf.set("job2AOutputPath", args[1]);

        Job job = Job.getInstance(conf, "Job2B - Compute MI from Job2A marginals and Job1 counts");
        job.setJarByClass(Job2B_ComputeMI.class);

        job.setMapperClass(MapperClass.class);
        job.setNumReduceTasks(0); // map-only job

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