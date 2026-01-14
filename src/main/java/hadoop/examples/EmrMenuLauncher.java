package hadoop.examples;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Scanner;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.ActionOnFailure;
import com.amazonaws.services.elasticmapreduce.model.Application;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;

/**
 * Interactive EMR launcher for your DIRT pipeline:
 *   Job1TripleCounts -> Job2A_Marginals -> Job2B_ComputeMI -> Job3A_PathVectors -> Job3B_ComputeSimilarity
 *
 * Requires:
 *   - AWS creds available (AWS CLI config / env vars / instance profile)
 *   - Your jar in S3 (default): s3://<bucket>/jars/ass3.jar
 */
public class EmrMenuLauncher {

    private static final String DEFAULT_BUCKET = "dspl-inputbucket";
    private static final String DEFAULT_JAR_KEY = "jars/ass3.jar";
    private static final String DEFAULT_SMALL_INPUT = "s3://dsp-ass3-first10-biarcs/";

    // Pick whatever your course expects. You can change at runtime in the menu.
    private static final String DEFAULT_EMR_RELEASE = "emr-6.15.0";

    private static final DateTimeFormatter RUN_ID_FMT =
            DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss").withZone(ZoneOffset.UTC);

    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);

        while (true) {
            System.out.println("\n=== EMR Launcher Menu ===");
            System.out.println("1) Run pipeline on SMALL input (10 files)");
            System.out.println("2) Run pipeline on CUSTOM S3 input prefix");
            System.out.println("3) Exit");
            System.out.print("Choose: ");

            String choice = sc.nextLine().trim();
            if ("3".equals(choice)) break;

            String region = prompt(sc, "AWS region", "us-east-1");
            String bucket = prompt(sc, "Output/Jar bucket", DEFAULT_BUCKET);
            String jarS3 = prompt(sc, "Jar S3 URI",
                    "s3://" + bucket + "/" + DEFAULT_JAR_KEY);

            String releaseLabel = prompt(sc, "EMR release label", DEFAULT_EMR_RELEASE);
            String masterType = prompt(sc, "Master instance type", "m4.large");
            String coreType   = prompt(sc, "Core instance type",   "m4.large");
            int coreCount = parseInt(prompt(sc, "Number of core nodes", "2"), 2);

            String keyName = prompt(sc, "EC2 KeyPair name (for SSH) [blank = none]", "");
            String subnetId = prompt(sc, "EC2 SubnetId [blank = default]", "");

            String input;
            if ("1".equals(choice)) {
                input = DEFAULT_SMALL_INPUT;
            } else if ("2".equals(choice)) {
                input = prompt(sc, "Input S3 prefix (must contain biarcs*.gz)", DEFAULT_SMALL_INPUT);
            } else {
                System.out.println("Unknown option.");
                continue;
            }

            try {
                runPipeline(region, bucket, jarS3, input, releaseLabel, masterType, coreType, coreCount, keyName, subnetId);
            } catch (Exception e) {
                System.err.println("FAILED: " + e.getMessage());
                e.printStackTrace(System.err);
            }
        }

        sc.close();
        System.out.println("Bye.");
    }

    private static void runPipeline(
            String regionName,
            String bucket,
            String jarPathS3,
            String inputS3,
            String releaseLabel,
            String masterType,
            String coreType,
            int coreCount,
            String ec2KeyName,
            String subnetId
    ) {
        AmazonElasticMapReduce emr = AmazonElasticMapReduceClientBuilder
                .standard()
                .withRegion(Regions.fromName(regionName))
                .withCredentials(DefaultAWSCredentialsProviderChain.getInstance())
                .build();

        String runId = RUN_ID_FMT.format(Instant.now());
        String baseOut = "s3://" + bucket + "/ass3/runs/" + runId;

        String job1Out  = baseOut + "/job1_triple_counts";
        String job2aOut = baseOut + "/job2a_marginals";
        String job2bOut = baseOut + "/job2b_mi";

        String job2b1Out = baseOut + "/job2b1_join_ps";

        String job3aOut = baseOut + "/job3a_path_vectors";
        String job3b0Out = baseOut + "/job3b0_slot_sums";
        String job3b1Out = baseOut + "/job3b1_pair_contrib";
        String job3b2Out = baseOut + "/job3b2_pair_numerators";
        String job3b3Out = baseOut + "/job3b3_slot_sims";
        String logsOut  = "s3://" + bucket + "/emr-logs/" + runId + "/";

        StepConfig step1 = new StepConfig()
                .withName("Job1 - TripleCounts")
                .withActionOnFailure(ActionOnFailure.TERMINATE_JOB_FLOW)
                .withHadoopJarStep(new HadoopJarStepConfig()
                        .withJar(jarPathS3)
                        .withMainClass("hadoop.examples.Job1TripleCounts")
                        .withArgs(Arrays.asList(inputS3, job1Out)));

        StepConfig step2a = new StepConfig()
                .withName("Job2A - Marginals")
                .withActionOnFailure(ActionOnFailure.TERMINATE_JOB_FLOW)
                .withHadoopJarStep(new HadoopJarStepConfig()
                        .withJar(jarPathS3)
                        .withMainClass("hadoop.examples.Job2A_Marginals")
                        .withArgs(Arrays.asList(job1Out, job2aOut)));

        // StepConfig step2b = new StepConfig()
        //         .withName("Job2B - ComputeMI")
        //         .withActionOnFailure(ActionOnFailure.TERMINATE_JOB_FLOW)
        //         .withHadoopJarStep(new HadoopJarStepConfig()
        //                 .withJar(jarPathS3)
        //                 .withMainClass("hadoop.examples.Job2B_ComputeMI")
        //                 .withArgs(Arrays.asList(job1Out, job2aOut, job2bOut)));

        StepConfig step2b1 = new StepConfig()
            .withName("Job2B1 - JoinPS")
            .withActionOnFailure(ActionOnFailure.TERMINATE_JOB_FLOW)
            .withHadoopJarStep(new HadoopJarStepConfig()
                .withJar(jarPathS3)
                .withMainClass("hadoop.examples.Job2B1_JoinPS")
                .withArgs(Arrays.asList(job1Out, job2aOut, job2b1Out)));

        StepConfig step2b2 = new StepConfig()
            .withName("Job2B2 - JoinSW + ComputeMI")
            .withActionOnFailure(ActionOnFailure.TERMINATE_JOB_FLOW)
            .withHadoopJarStep(new HadoopJarStepConfig()
                .withJar(jarPathS3)
                .withMainClass("hadoop.examples.Job2B2_JoinSWComputeMI")
                .withArgs(Arrays.asList(job2b1Out, job2aOut, job2bOut)));

        StepConfig step3a = new StepConfig()
                .withName("Job3A - PathVectors")
                .withActionOnFailure(ActionOnFailure.TERMINATE_JOB_FLOW)
                .withHadoopJarStep(new HadoopJarStepConfig()
                        .withJar(jarPathS3)
                        .withMainClass("hadoop.examples.Job3A_PathVectors")
                        .withArgs(Arrays.asList(job2bOut, job3aOut)));

        // StepConfig step3b = new StepConfig()
        //         .withName("Job3B - ComputeSimilarity")
        //         .withActionOnFailure(ActionOnFailure.TERMINATE_JOB_FLOW)
        //         .withHadoopJarStep(new HadoopJarStepConfig()
        //                 .withJar(jarPathS3)
        //                 .withMainClass("hadoop.examples.Job3B_ComputeSimilarity")
        //                 .withArgs(Arrays.asList(job3aOut, job3aOut, job3bOut)));

                StepConfig step3b0 = new StepConfig()
            .withName("Job3B0 - SlotSums")
            .withActionOnFailure(ActionOnFailure.TERMINATE_JOB_FLOW)
            .withHadoopJarStep(new HadoopJarStepConfig()
                .withJar(jarPathS3)
                .withMainClass("hadoop.examples.Job3B0_SlotSums")
                .withArgs(Arrays.asList(job3aOut, job3b0Out)));

        StepConfig step3b1 = new StepConfig()
            .withName("Job3B1 - FeaturePairContrib")
            .withActionOnFailure(ActionOnFailure.TERMINATE_JOB_FLOW)
            .withHadoopJarStep(new HadoopJarStepConfig()
                .withJar(jarPathS3)
                .withMainClass("hadoop.examples.Job3B1_FeaturePairContrib")
                .withArgs(Arrays.asList(job3aOut, job3b1Out)));

        StepConfig step3b2 = new StepConfig()
            .withName("Job3B2 - AggregatePairContrib")
            .withActionOnFailure(ActionOnFailure.TERMINATE_JOB_FLOW)
            .withHadoopJarStep(new HadoopJarStepConfig()
                .withJar(jarPathS3)
                .withMainClass("hadoop.examples.Job3B2_AggregatePairContrib")
                .withArgs(Arrays.asList(job3b1Out, job3b2Out)));

        StepConfig step3b3 = new StepConfig()
            .withName("Job3B3 - FinalSlotSims")
            .withActionOnFailure(ActionOnFailure.TERMINATE_JOB_FLOW)
            .withHadoopJarStep(new HadoopJarStepConfig()
                .withJar(jarPathS3)
                .withMainClass("hadoop.examples.Job3B3_FinalSimilarity")
                .withArgs(Arrays.asList(job3b2Out, job3b0Out, job3b3Out)));

        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withMasterInstanceType(masterType)
                .withSlaveInstanceType(coreType)
                .withInstanceCount(1 + coreCount)
                .withKeepJobFlowAliveWhenNoSteps(false)
                // placement is optional; zone must be like "us-east-1a"
                ;

        if (ec2KeyName != null && !ec2KeyName.trim().isEmpty()) {
            instances = instances.withEc2KeyName(ec2KeyName.trim());
        }
        if (subnetId != null && !subnetId.trim().isEmpty()) {
            instances = instances.withEc2SubnetId(subnetId.trim());
        }

        RunJobFlowRequest req = new RunJobFlowRequest()
                .withName("dspl-ass3-dirt-" + runId)
                .withReleaseLabel(releaseLabel)
                .withInstances(instances)
                .withApplications(new Application().withName("Hadoop"))
                .withSteps(step1, step2a, step2b1, step2b2, step3a, step3b0, step3b1, step3b2, step3b3)
                .withLogUri(logsOut)
                .withServiceRole("EMR_DefaultRole")
                .withJobFlowRole("EMR_EC2_DefaultRole");

        RunJobFlowResult res = emr.runJobFlow(req);

        System.out.println("\nStarted cluster: " + res.getJobFlowId());
        System.out.println("Input:  " + inputS3);
        System.out.println("Jar:    " + jarPathS3);
        System.out.println("Logs:   " + logsOut);
        System.out.println("Job1:   " + job1Out);
        System.out.println("Job2A:  " + job2aOut);
        System.out.println("Job2B1: " + job2b1Out);
        System.out.println("Job2B2: " + job2bOut);
        System.out.println("Job3A:  " + job3aOut);
        // System.out.println("Job3B:  " + job3bOut);
    }

    private static String prompt(Scanner sc, String label, String def) {
        System.out.print(label + " [" + def + "]: ");
        String s = sc.nextLine();
        if (s == null) return def;
        s = s.trim();
        return s.isEmpty() ? def : s;
    }

    private static int parseInt(String s, int def) {
        try { return Integer.parseInt(s.trim()); }
        catch (Exception e) { return def; }
    }
}
