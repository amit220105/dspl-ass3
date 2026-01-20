package hadoop.examples;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.PlacementType;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;

public class JobFlowAss3 {

    private static String normalizeRunPrefix(String s, String bucket) {
        if (s == null || s.isEmpty()) throw new IllegalArgumentException("runPrefix is empty");

        // If user passed just a key prefix like "ass3/runs/test2", convert to s3://bucket/ass3/runs/test2
        if (!s.startsWith("s3://")) {
            s = s.startsWith("/") ? s.substring(1) : s;
            s = "s3://" + bucket + "/" + s;
        }

        // Remove trailing slash to avoid "//job1" etc.
        while (s.endsWith("/")) s = s.substring(0, s.length() - 1);
        return s;
    }

    private static String normalizeInput(String s, String bucket) {
        if (s == null || s.isEmpty()) throw new IllegalArgumentException("inputPrefix is empty");

        // Allow either full s3://... or bucket-relative prefix
        if (!s.startsWith("s3://")) {
            s = s.startsWith("/") ? s.substring(1) : s;
            s = "s3://" + bucket + "/" + s;
        }
        return s;
    }

    private static String requireS3Uri(String s, String name) {
        if (s == null || s.isEmpty()) throw new IllegalArgumentException(name + " is empty");
        if (!s.startsWith("s3://")) throw new IllegalArgumentException(name + " must be an s3:// URI, got: " + s);
        return s;
    }

    public static void main(String[] args) throws Exception {
        // Expected args:
        // <region> <bucket> <jarPath> <inputPrefix> <runPrefix> <positivePredsS3> <negativePredsS3>
        if (args.length != 7) {
            System.err.println("Usage: JobFlowAss3 <region> <bucket> <jarPath> <inputPrefix> <runPrefix> <positivePredsS3> <negativePredsS3>");
            System.exit(1);
        }

        String regionName = args[0];            // e.g. us-east-1
        String bucket = args[1];                // e.g. amit-dspl-3
        String jarPath = args[2];               // e.g. s3://amit-dspl-3/jars/dspl-ass3.jar
        String input = normalizeInput(args[3], bucket);
        String runPrefix = normalizeRunPrefix(args[4], bucket);

        String posPreds = requireS3Uri(args[5], "positivePredsS3");
        String negPreds = requireS3Uri(args[6], "negativePredsS3");

        // Outputs (ALL are S3!)
        String job1Out  = runPrefix + "/job1_triples";
        String job2aOut = runPrefix + "/job2a_marginals";
        String job2bOut = runPrefix + "/job2b_mi";

        // Similarity outputs
        String job3Out  = runPrefix + "/job3_denoms";
        String job4Out  = runPrefix + "/job4_numerators";
        String job5Out  = runPrefix + "/job5_similarity";

        // Denoms file path (since Job3 is forced to 1 reducer, it's always part-r-00000)
        String job3DenomsPart = job3Out + "/part-r-00000";

        String logUri = "s3://" + bucket + "/logs/ass3/";

        AmazonElasticMapReduce emr = AmazonElasticMapReduceClientBuilder
                .standard()
                .withRegion(Regions.fromName(regionName))
                .withCredentials(new DefaultAWSCredentialsProviderChain())
                .build();

        // Step 1: Job1TripleCounts  (input -> job1Out)
        HadoopJarStepConfig step1Jar = new HadoopJarStepConfig()
                .withJar(jarPath)
                .withMainClass("hadoop.examples.Job1TripleCounts")
                .withArgs(input, job1Out);

        StepConfig step1 = new StepConfig()
                .withName("ASS3-Job1-TripleCounts")
                .withHadoopJarStep(step1Jar)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        // Step 2: Job2A_Marginals   (job1Out -> job2aOut)
        HadoopJarStepConfig step2Jar = new HadoopJarStepConfig()
                .withJar(jarPath)
                .withMainClass("hadoop.examples.Job2A_Marginals")
                .withArgs(job1Out, job2aOut);

        StepConfig step2 = new StepConfig()
                .withName("ASS3-Job2A-Marginals")
                .withHadoopJarStep(step2Jar)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        // Step 3: Job2B_ComputeMI   (job1Out + job2aOut -> job2bOut)
        HadoopJarStepConfig step3Jar = new HadoopJarStepConfig()
                .withJar(jarPath)
                .withMainClass("hadoop.examples.Job2B_ComputeMI")
                .withArgs(job1Out, job2aOut, job2bOut);

        StepConfig step3 = new StepConfig()
                .withName("ASS3-Job2B-ComputeMI")
                .withHadoopJarStep(step3Jar)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        // Step 4: Job3_Denoms (job2bOut -> job3Out)
        // IMPORTANT: Job3_Denoms must do setNumReduceTasks(1) inside its main.
        HadoopJarStepConfig step4Jar = new HadoopJarStepConfig()
                .withJar(jarPath)
                .withMainClass("hadoop.examples.Job3_Denoms")
                .withArgs(job2bOut, job3Out);

        StepConfig step4 = new StepConfig()
                .withName("ASS3-Job3-Denoms")
                .withHadoopJarStep(step4Jar)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        // Step 5: Job4_TestNumerators (job2bOut + testset cache -> job4Out)
        HadoopJarStepConfig step5Jar = new HadoopJarStepConfig()
                .withJar(jarPath)
                .withMainClass("hadoop.examples.Job4_TestNumerators")
                .withArgs(job2bOut, posPreds, negPreds, job4Out);

        StepConfig step5 = new StepConfig()
                .withName("ASS3-Job4-TestNumerators")
                .withHadoopJarStep(step5Jar)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        // Step 6: Job5_FinalSimilarity (job4Out + denoms cache + testset cache -> job5Out)
        HadoopJarStepConfig step6Jar = new HadoopJarStepConfig()
                .withJar(jarPath)
                .withMainClass("hadoop.examples.Job5_FinalSimilarity")
                .withArgs(job4Out, job3DenomsPart, posPreds, negPreds, job5Out);

        StepConfig step6 = new StepConfig()
                .withName("ASS3-Job5-FinalSimilarity")
                .withHadoopJarStep(step6Jar)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        // Cluster config (adjust if course requires other types/count)
        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(2)
                .withMasterInstanceType("m4.large")
                .withSlaveInstanceType("m4.large")
                .withEc2KeyName("vockey")
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType(regionName + "a"));

        RunJobFlowRequest request = new RunJobFlowRequest()
                .withName("ASS3-Similarity-Pipeline")
                .withReleaseLabel("emr-5.36.0")
                .withInstances(instances)
                .withSteps(step1, step2, step3, step4, step5, step6)
                .withLogUri(logUri)
                .withServiceRole("EMR_DefaultRole")
                .withJobFlowRole("EMR_EC2_DefaultRole");

        RunJobFlowResult result = emr.runJobFlow(request);
        System.out.println("Started EMR job flow with id: " + result.getJobFlowId());

        // Print outputs so you can immediately check S3
        System.out.println("Outputs:");
        System.out.println("  Job1:  " + job1Out);
        System.out.println("  Job2A: " + job2aOut);
        System.out.println("  Job2B: " + job2bOut);
        System.out.println("  Job3:  " + job3Out + "  (cache file: " + job3DenomsPart + ")");
        System.out.println("  Job4:  " + job4Out);
        System.out.println("  Job5:  " + job5Out);
    }
}