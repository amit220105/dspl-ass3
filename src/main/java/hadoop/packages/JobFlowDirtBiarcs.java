package hadoop.packages;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;

/**
 * Starts an EMR cluster and runs your 3-step DIRT pipeline:
 *  1) hadoop.examples.Job1TripleCounts
 *  2) hadoop.examples.Job2A_Marginals
 *  3) packages.Job2B_ComputeMI   (as in your code: package packages;)
 *
 * Usage:
 *   JobFlowDirtBiarcs <region> <bucket> <biarcs-input-s3-prefix>
 *
 * Example:
 *   java -cp target/hadoop-examples-1.0-SNAPSHOT.jar hadoop.examples.JobFlowDirtBiarcs us-east-1 my-bucket s3://my-bucket/biarcs/input/
 *
 * Requirements:
 *   - Your fat jar is uploaded to: s3://<bucket>/jars/hadoop-examples-1.0-SNAPSHOT.jar
 *   - AWS credentials available via DefaultAWSCredentialsProviderChain (AWS CLI configured, env vars, or instance profile)
 */
public class JobFlowDirtBiarcs {

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: JobFlowDirtBiarcs <region> <bucket> <biarcs-input-s3-prefix>");
            System.exit(1);
        }

        String regionName = args[0];
        String bucket = args[1];
        String biarcsInput = args[2];

        // Must exist in S3:
        String jarPath = "s3://" + bucket + "/jars/hadoop-examples-1.0-SNAPSHOT.jar";

        // Outputs (must NOT already exist in S3, or Hadoop will fail)
        String job1Out  = "s3://" + bucket + "/dirt/job1_triple_counts";
        String job2aOut = "s3://" + bucket + "/dirt/job2a_marginals";
        String job2bOut = "s3://" + bucket + "/dirt/job2b_mi";

        AmazonElasticMapReduce emr = AmazonElasticMapReduceClientBuilder
                .standard()
                .withRegion(Regions.fromName(regionName))
                .withCredentials(new DefaultAWSCredentialsProviderChain())
                .build();

        // Step 1: Triple counts |p,slot,w|
        HadoopJarStepConfig job1Jar = new HadoopJarStepConfig()
                .withJar(jarPath)
                .withMainClass("hadoop.packages.Job1TripleCounts")
                .withArgs(biarcsInput, job1Out);

        StepConfig job1Step = new StepConfig()
                .withName("DIRT-Job1-TripleCounts")
                .withHadoopJarStep(job1Jar)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        // Step 2A: Marginals PS/SW/S
        HadoopJarStepConfig job2aJar = new HadoopJarStepConfig()
                .withJar(jarPath)
                .withMainClass("hadoop.packages.Job2A_Marginals")
                .withArgs(job1Out, job2aOut);

        StepConfig job2aStep = new StepConfig()
                .withName("DIRT-Job2A-Marginals")
                .withHadoopJarStep(job2aJar)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        // Step 2B: Compute MI (NOTE: your class is in package "packages")
        HadoopJarStepConfig job2bJar = new HadoopJarStepConfig()
                .withJar(jarPath)
                .withMainClass("hadoop.packages.Job2B_ComputeMI")
                .withArgs(job1Out, job2aOut, job2bOut);

        StepConfig job2bStep = new StepConfig()
                .withName("DIRT-Job2B-ComputeMI")
                .withHadoopJarStep(job2bJar)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        // Cluster config (copied one-to-one from your JobFlowNgram style)
        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
        .withInstanceCount(2)
        .withMasterInstanceType("m5.large")
        .withSlaveInstanceType("m5.large")
        .withKeepJobFlowAliveWhenNoSteps(false);

        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("DIRT-Biarcs-Pipeline")
                .withReleaseLabel("emr-5.36.0")
                .withInstances(instances)
                .withSteps(job1Step, job2aStep, job2bStep)
                .withLogUri("s3://" + bucket + "/logs/")
                .withServiceRole("EMR_DefaultRole")
                .withJobFlowRole("EMR_EC2_DefaultRole");

        RunJobFlowResult result = emr.runJobFlow(runFlowRequest);
        System.out.println("Started EMR job flow with id: " + result.getJobFlowId());
        System.out.println("Job1 out:  " + job1Out);
        System.out.println("Job2A out: " + job2aOut);
        System.out.println("Job2B out: " + job2bOut);
    }
}
