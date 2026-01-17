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
 * Interactive EMR launcher for a 4-step DIRT pipeline:
 *   1) Job1TripleCounts
 *   2) Job2A_Marginals
 *   3) Job3_ComputeMIAndSlotSums   (outputs: <out>/mi and <out>/sums)
 *   4) Job4_TestSetSimilarityDirect (uses MI + sums + testset to output similarities)
 *
 * Outputs:
 *  - MI(p,slot,w) table (for future similarity queries)
 *  - Test-set pair similarities
 */
public class EmrMenuLauncher {

    private static final String DEFAULT_BUCKET = "dspl-inputbucket";
    private static final String DEFAULT_JAR_KEY = "jars/ass3.jar";
    private static final String DEFAULT_SMALL_INPUT = "s3://dsp-ass3-first10-biarcs/";

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
            String testSetS3 = prompt(sc, "Test-set pairs S3 URI", "s3://" + bucket + "/ass3/testset.txt");

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
                runPipeline(region, bucket, jarS3, input, releaseLabel, masterType, coreType, coreCount, keyName, subnetId, testSetS3);
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
            String subnetId,
            String testSetS3
    ) {
        AmazonElasticMapReduce emr = AmazonElasticMapReduceClientBuilder
                .standard()
                .withRegion(Regions.fromName(regionName))
                .withCredentials(DefaultAWSCredentialsProviderChain.getInstance())
                .build();

        String runId = RUN_ID_FMT.format(Instant.now());
        String baseOut = "s3://" + bucket + "/ass3/runs/" + runId;

        // 4-step outputs
        String job1Out  = baseOut + "/job1_triple_counts";
        String job2aOut = baseOut + "/job2a_marginals";
        String job3Out  = baseOut + "/job3_mi_and_sums";          // contains /mi and /sums
        String job4Out  = baseOut + "/job4_testset_similarity";

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

        StepConfig step3 = new StepConfig()
                .withName("Job3 - ComputeMI + SlotSums")
                .withActionOnFailure(ActionOnFailure.TERMINATE_JOB_FLOW)
                .withHadoopJarStep(new HadoopJarStepConfig()
                        .withJar(jarPathS3)
                        .withMainClass("hadoop.examples.Job3_ComputeMIAndSlotSums")
                        .withArgs(Arrays.asList(job1Out, job2aOut, job3Out)));

        StepConfig step4 = new StepConfig()
                .withName("Job4 - TestSetSimilarity (Direct)")
                .withActionOnFailure(ActionOnFailure.TERMINATE_JOB_FLOW)
                .withHadoopJarStep(new HadoopJarStepConfig()
                        .withJar(jarPathS3)
                        .withMainClass("hadoop.examples.Job4_TestSetSimilarityDirect")
                        .withArgs(Arrays.asList(
                                job3Out + "/mi",
                                job3Out + "/sums",
                                testSetS3,
                                job4Out
                        )));

        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withMasterInstanceType(masterType)
                .withSlaveInstanceType(coreType)
                .withInstanceCount(1 + coreCount)
                .withKeepJobFlowAliveWhenNoSteps(false);

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
                .withSteps(step1, step2a, step3, step4)
                .withLogUri(logsOut)
                .withServiceRole("EMR_DefaultRole")
                .withJobFlowRole("EMR_EC2_DefaultRole");

        RunJobFlowResult res = emr.runJobFlow(req);

        System.out.println("\nStarted cluster: " + res.getJobFlowId());
        System.out.println("Input:  " + inputS3);
        System.out.println("Jar:    " + jarPathS3);
        System.out.println("Logs:   " + logsOut);

        System.out.println("Job1 (TripleCounts): " + job1Out);
        System.out.println("Job2A (Marginals):   " + job2aOut);

        System.out.println("MI table (Job3):     " + job3Out + "/mi");
        System.out.println("Slot sums (Job3):    " + job3Out + "/sums");

        System.out.println("TestSet Similarities (Job4): " + job4Out);
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
