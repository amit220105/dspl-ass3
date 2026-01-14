package hadoop.examples;

import java.io.IOException;
import java.util.HashMap;
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


/**
 * Job3A: Aggregate MI scores per path and compute slot sums for Lin similarity.
 * 
 * Input: Job2B output (p \t slot \t w \t MI)
 * Output: p \t slot \t w \t MI \t slotSum
 *         where slotSum = sum of all MI values for (p, slot)
 */
public class Job3A_PathVectors {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
        private final Text outKey = new Text();
        private final Text outValue = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;
            
            String[] parts = line.split("\t");
            if (parts.length != 4) return; // invalid line
            
            // Job2B output: p \t slot \t w \t MI
            String p = parts[0];
            String slot = parts[1];
            String w = parts[2];
            String mi = parts[3];
            
            // Emit: (p, slot) -> (w, MI)
            outKey.set(p + "\t" + slot);
            outValue.set(w + "\t" + mi);
            context.write(outKey, outValue);
        }
    }

    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
        private final Text outValue = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            
            // key = "p \t slot"
            // values = list of "w \t MI"
            
            // First pass: collect all (w, MI) pairs and compute sum of MI
            Map<String, Double> features = new HashMap<>();
            double sumMI = 0.0;
            
            for (Text val : values) {
                String[] parts = val.toString().split("\t");
                if (parts.length != 2) continue;
                
                String w = parts[0];
                double mi;
                try {
                    mi = Double.parseDouble(parts[1]);
                } catch (NumberFormatException e) {
                    continue;
                }
                
                features.put(w, mi);
                sumMI += mi;
            }
            
            // Second pass: emit each feature with the slot sum
            // Output: p \t slot \t w \t MI \t slotSum
            String[] keyParts = key.toString().split("\t");
            if (keyParts.length != 2) return;
            
            String p = keyParts[0];
            String slot = keyParts[1];
            
            for (Map.Entry<String, Double> entry : features.entrySet()) {
                String w = entry.getKey();
                double mi = entry.getValue();
                
                outValue.set(slot + "\t" + w + "\t" + mi + "\t" + sumMI);
                context.write(new Text(p), outValue);
            }
        }
    }

    public static class CombinerClass extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // Combiner: just pass through, can't aggregate without seeing all features
            for (Text val : values) {
                context.write(key, val);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        System.err.println("Job3A_PathVectors args=" + java.util.Arrays.toString(args));

        int off = 0;
        if (args.length >= 3 && args[0].contains(".") && !args[0].startsWith("s3://")) {
            off = 1;
        }

        if (args.length < off + 2) {
            System.err.println("invalid input. args=" + java.util.Arrays.toString(args));
            System.exit(1);
        }

        String in = args[off];   // Job2B output
        String out = args[off + 1];

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Job3A - Path Feature Vectors with Slot Sums");
        job.setJarByClass(Job3A_PathVectors.class);

        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);

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
