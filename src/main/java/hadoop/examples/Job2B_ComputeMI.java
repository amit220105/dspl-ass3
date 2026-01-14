package hadoop.examples;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
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
            FileSystem fs = FileSystem.get(job2APath.toUri(), conf);
            for (FileStatus status : fs.listStatus(job2APath)) {
                if (!status.isFile()) continue;
                String fileName = status.getPath().getName();
                // skip _SUCCESS and hidden temp files
                if (fileName.startsWith("_") || fileName.startsWith(".")) continue;
                // read only reducer/mapper outputs
                if (!fileName.startsWith("part-")) continue;
                try (FSDataInputStream in = fs.open(status.getPath());
                    BufferedReader br = new BufferedReader(new InputStreamReader(in))) {

                    String line;
                    while ((line = br.readLine()) != null) {
                        line = line.trim();
                        if (line.isEmpty()) continue;

                        String[] parts = line.split("\t");
                        if (parts.length < 3) continue;

                        if (parts[0].equals("PS")) {
                            if (parts.length != 4) continue;
                            long ps = Long.parseLong(parts[3]);
                            if (ps > 0) psMap.put(parts[1] + "\t" + parts[2], ps);

                        } else if (parts[0].equals("SW")) {
                            if (parts.length != 4) continue;
                            long sw = Long.parseLong(parts[3]);
                            if (sw > 0) swMap.put(parts[1] + "\t" + parts[2], sw);

                        } else if (parts[0].equals("S")) {
                            if (parts.length != 3) continue;
                            long s = Long.parseLong(parts[2]);
                            if (s > 0) sMap.put(parts[1], s);
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
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
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
            if (count <= 0 || ps <= 0 || sw <= 0 || s <= 0) return;

            double mi = Math.log((double)count * (double)s / ((double)ps * (double)sw));
            if (mi <= 0.0) return; // PMI+ (keep only positive PMI)
            outKey.set(p + "\t" + slot + "\t" + w);
            outValue.set(Double.toString(mi));
            context.write(outKey, outValue);
        }    
    }



public static void main(String[] args) throws Exception {
    System.err.println("Job2B_ComputeMI args=" + java.util.Arrays.toString(args));

    int off = 0;
    // If EMR injected the class name as args[0], skip it
    if (args.length >= 4 && args[0].contains(".") && !args[0].startsWith("s3://")) {
        off = 1;
    }

    if (args.length < off + 3) {
        System.err.println("invalid input. args=" + java.util.Arrays.toString(args));
        System.exit(1);
    }

    String job1Out  = args[off];       // input for Job2B = Job1 output
    String job2aOut = args[off + 1];   // Job2A output path (for loading marginals)
    String job2bOut = args[off + 2];   // output for Job2B

    Configuration conf = new Configuration();
    conf.set("job2AOutputPath", job2aOut);

    Job job = Job.getInstance(conf, "Job2B - Compute MI from Job2A marginals and Job1 counts");
    job.setJarByClass(Job2B_ComputeMI.class);

    job.setMapperClass(MapperClass.class);
    job.setNumReduceTasks(0); // map-only job

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    job.setInputFormatClass(TextInputFormat.class);
    LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

    // Input = Job1 output
    FileInputFormat.addInputPath(job, new Path(job1Out));

    // Output (delete if exists to avoid FileAlreadyExistsException)
    Path outPath = new Path(job2bOut);
    FileSystem outFs = FileSystem.get(outPath.toUri(), conf);
    if (outFs.exists(outPath)) {
        outFs.delete(outPath, true);
    }
    FileOutputFormat.setOutputPath(job, outPath);

    System.exit(job.waitForCompletion(true) ? 0 : 1);
}
}