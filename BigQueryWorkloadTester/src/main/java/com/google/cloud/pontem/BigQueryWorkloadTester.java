/*
 * Copyright 2019 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.pontem;

import com.google.cloud.pontem.auth.BigQueryCredentialManager;
import com.google.cloud.pontem.benchmark.Benchmark;
import com.google.cloud.pontem.benchmark.RatioBasedWorkloadBenchmark;
import com.google.cloud.pontem.benchmark.WorkloadBenchmark;
import com.google.cloud.pontem.benchmark.backends.BigQueryBackend;
import com.google.cloud.pontem.benchmark.backends.BigQueryBackendFactory;
import com.google.cloud.pontem.benchmark.runners.ConcurrentWorkloadRunnerFactory;
import com.google.cloud.pontem.config.Configuration;
import com.google.cloud.pontem.config.WorkloadSettings;
import com.google.cloud.pontem.model.FailedRun;
import com.google.cloud.pontem.model.WorkloadResult;
import com.google.cloud.pontem.result.JsonResultProcessor;
import com.google.cloud.pontem.result.JsonResultProcessorFactory;
import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import static spark.Spark.*;


/** Tool to Benchmark BigQuery Workloads */
public final class BigQueryWorkloadTester {

  private static final Logger logger = Logger.getLogger(BigQueryWorkloadTester.class.getName());

  private static String results = "";
  private static String getResults() {
      return results;
  }
  private static Gson gson = new Gson();

  private static String runWorkloadsWithConfiguration(String configYaml) {
    logger.info("Loading config from string: " + configYaml);
    Configuration.loadConfigFromString(configYaml);
    Configuration config = Configuration.getInstance();
    BigQueryCredentialManager bigQueryCredentialManager = new BigQueryCredentialManager();

    logger.info("Starting execution");
    results = "";
    List<WorkloadResult> workloadResults = new ArrayList<>();
    try {
      for (WorkloadSettings workload : config.getWorkloads()) {
        BigQueryBackend bigQueryBackend =
                BigQueryBackendFactory.getBigQueryBackend(bigQueryCredentialManager, workload);
        ConcurrentWorkloadRunnerFactory runnerFactory =
                new ConcurrentWorkloadRunnerFactory(bigQueryBackend);
        Benchmark benchmark = getBenchmark(config, runnerFactory);

        int concurrencyLevel = config.getConcurrencyLevel();
        Preconditions.checkArgument(
                concurrencyLevel > 0, "Concurrency Level must be higher than 0!");
        workloadResults.addAll(benchmark.run(workload, concurrencyLevel));

        logger.info("Finished benchmarking phase.");

        if (!workload.getOutputFileName().isEmpty()) {
          logger.info("processing results. Writing to '" + workload.getOutputFileName() + "'");
          String outputPath =
              config.getOutputFileFolder() + File.separator + workload.getOutputFileName();
          JsonResultProcessor jsonResultProcessor =
              JsonResultProcessorFactory.getJsonResultProcessor();
          jsonResultProcessor.run(outputPath, workloadResults);
        }
      }
      results = gson.toJson(workloadResults);
    } catch (Throwable t) {
      logger.log(Level.SEVERE, "Caught Exception while executing the Workload Benchmark: ", t);
      FailedRun.Builder failedRunBuilder = FailedRun.newBuilder();
      List<String> stacktraceString = new ArrayList<>();
      for (StackTraceElement e: t.getStackTrace()) {
        stacktraceString.add(e.toString());
      }
      failedRunBuilder.setException(t);
      results = gson.toJson(failedRunBuilder.build());
    }
    return results;
  }

  private static String runWorkloads() {
    logger.info("Loading config");
    Configuration.loadConfig("config.yaml");
    Configuration config = Configuration.getInstance();
    BigQueryCredentialManager bigQueryCredentialManager = new BigQueryCredentialManager();

    logger.info("Starting execution");
    try {
      for (WorkloadSettings workload : config.getWorkloads()) {
        BigQueryBackend bigQueryBackend =
            BigQueryBackendFactory.getBigQueryBackend(bigQueryCredentialManager, workload);
        ConcurrentWorkloadRunnerFactory runnerFactory =
            new ConcurrentWorkloadRunnerFactory(bigQueryBackend);
        Benchmark benchmark = getBenchmark(config, runnerFactory);

        int concurrencyLevel = config.getConcurrencyLevel();
        Preconditions.checkArgument(
            concurrencyLevel > 0, "Concurrency Level must be higher than 0!");
        List<WorkloadResult> workloadResults = benchmark.run(workload, concurrencyLevel);


        logger.info("Finished benchmarking phase.");
        if (!workload.getOutputFileName().isEmpty()) {
          logger.info("processing results. Writing to '" + workload.getOutputFileName() + "'");
          String outputPath =
                  config.getOutputFileFolder() + File.separator + workload.getOutputFileName();
          JsonResultProcessor jsonResultProcessor =
                  JsonResultProcessorFactory.getJsonResultProcessor();
          jsonResultProcessor.run(outputPath, workloadResults);

          Path path = Paths.get(outputPath);
          List<String> lines = Files.readAllLines(path);
          for (String line: lines) {
            results = results.concat(line);
          }
        }
      }
    } catch (Throwable t) {
      logger.log(Level.SEVERE, "Caught Exception while executing the Workload Benchmark: ", t);
      FailedRun.Builder failedRunBuilder = FailedRun.newBuilder();
      List<String> stacktraceString = new ArrayList<>();
      for (StackTraceElement e: t.getStackTrace()) {
        stacktraceString.add(e.toString());
      }
      failedRunBuilder.setException(t);
      results = gson.toJson(failedRunBuilder.build());
    }
    return results;
  }

  /** Main entry point for benchmark. Sets up the object graph and kicks-off execution. */
  public static void main(String[] args) {
    logger.info("Welcome to BigQuery Workload Tester!");
    Map<String, String> env = System.getenv();

    if (env.containsKey("PORT")) {
      port(Integer.parseInt(env.get("PORT")));
    }

    staticFiles.location("/ux/dist");
    redirect.get("/", "/configuration");
    get("/results", (req, res) -> getResults());
    post("/run", (req, res) -> {
      JsonParser p = new JsonParser();
      JsonElement e = p.parse(req.body());
      return runWorkloadsWithConfiguration(e.getAsJsonObject().get("configuration").getAsString());
    }) ;
    logger.info("Finished Workload Tester execution");
  }

  private static Benchmark getBenchmark(
      final Configuration config, final ConcurrentWorkloadRunnerFactory runnerFactory) {
    Benchmark benchmark;
    if (config.isRatioBasedBenchmark()) {
      benchmark = new RatioBasedWorkloadBenchmark(runnerFactory, config.getBenchmarkRatios());
    } else {
      benchmark = new WorkloadBenchmark(runnerFactory);
    }

    return benchmark;
  }
}
