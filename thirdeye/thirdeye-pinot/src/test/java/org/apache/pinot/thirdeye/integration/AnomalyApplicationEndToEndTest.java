/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.pinot.thirdeye.integration;

import org.apache.pinot.thirdeye.anomaly.ThirdEyeAnomalyConfiguration;
import org.apache.pinot.thirdeye.anomaly.classification.classifier.AnomalyClassifierFactory;
import org.apache.pinot.thirdeye.anomaly.classification.ClassificationJobScheduler;
import org.apache.pinot.thirdeye.anomaly.job.JobConstants.JobStatus;
import org.apache.pinot.thirdeye.anomaly.monitor.MonitorConfiguration;
import org.apache.pinot.thirdeye.anomaly.monitor.MonitorConstants;
import org.apache.pinot.thirdeye.anomaly.monitor.MonitorJobScheduler;
import org.apache.pinot.thirdeye.anomaly.task.TaskConstants.TaskStatus;
import org.apache.pinot.thirdeye.anomaly.task.TaskConstants.TaskType;
import org.apache.pinot.thirdeye.anomaly.task.TaskDriver;
import org.apache.pinot.thirdeye.anomaly.task.TaskDriverConfiguration;
import org.apache.pinot.thirdeye.anomaly.task.TaskInfoFactory;
import org.apache.pinot.thirdeye.common.time.TimeGranularity;
import org.apache.pinot.thirdeye.completeness.checker.DataCompletenessConstants.DataCompletenessType;
import org.apache.pinot.thirdeye.completeness.checker.DataCompletenessScheduler;
import org.apache.pinot.thirdeye.completeness.checker.DataCompletenessTaskInfo;
import org.apache.pinot.thirdeye.datalayer.DaoTestUtils;
import org.apache.pinot.thirdeye.datalayer.bao.DAOTestBase;
import org.apache.pinot.thirdeye.datalayer.bao.DatasetConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.EvaluationManager;
import org.apache.pinot.thirdeye.datalayer.bao.EventManager;
import org.apache.pinot.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import org.apache.pinot.thirdeye.datalayer.bao.MetricConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.TaskManager;
import org.apache.pinot.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import org.apache.pinot.thirdeye.datalayer.dto.JobDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.datalayer.dto.TaskDTO;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.apache.pinot.thirdeye.datasource.ThirdEyeCacheRegistry;
import org.apache.pinot.thirdeye.datasource.loader.AggregationLoader;
import org.apache.pinot.thirdeye.datasource.loader.DefaultAggregationLoader;
import org.apache.pinot.thirdeye.datasource.loader.DefaultTimeSeriesLoader;
import org.apache.pinot.thirdeye.datasource.loader.TimeSeriesLoader;
import org.apache.pinot.thirdeye.detection.DataProvider;
import org.apache.pinot.thirdeye.detection.DefaultDataProvider;
import org.apache.pinot.thirdeye.detection.DetectionPipelineLoader;
import org.apache.pinot.thirdeye.detection.DetectionPipelineScheduler;
import org.apache.pinot.thirdeye.detection.alert.DetectionAlertScheduler;
import org.apache.pinot.thirdeye.detector.email.filter.AlertFilterFactory;
import org.apache.pinot.thirdeye.detector.function.AnomalyFunctionFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.quartz.SchedulerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.thirdeye.datalayer.DaoTestUtils.*;


public class AnomalyApplicationEndToEndTest {
  private static final Logger LOG = LoggerFactory.getLogger(AnomalyApplicationEndToEndTest.class);
  private DetectionPipelineScheduler detectionScheduler = null;
  private TaskDriver taskDriver = null;
  private MonitorJobScheduler monitorJobScheduler = null;
  private DetectionAlertScheduler alertScheduler = null;
  private DataCompletenessScheduler dataCompletenessScheduler = null;
  private ClassificationJobScheduler classificationJobScheduler = null;
  private AnomalyFunctionFactory anomalyFunctionFactory = null;
  private AlertFilterFactory alertFilterFactory = null;
  private AnomalyClassifierFactory anomalyClassifierFactory = null;
  private ThirdEyeAnomalyConfiguration thirdeyeAnomalyConfig;
  private List<TaskDTO> tasks;
  private List<JobDTO> jobs;
  private long functionId;

  private int id = 0;
  private String detectionConfigFile = "/sample-detection-config.yml";
  private String alertConfigFile = "/sample-alert-config.yml";
  private String dashboardHost = "http://localhost:8080/dashboard";
  private String metric = "cost";
  private String collection = "test-collection";
  private DAOTestBase testDAOProvider = null;
  private DAORegistry daoRegistry = null;
  private MetricConfigManager metricDAO;
  private DatasetConfigManager datasetDAO;
  private EventManager eventDAO;
  private MergedAnomalyResultManager anomalyDAO;
  private EvaluationManager evaluationDAO;
  private DetectionPipelineLoader detectionPipelineLoader;

  @BeforeClass
  void beforeClass() {
    testDAOProvider = DAOTestBase.getInstance();
    daoRegistry = DAORegistry.getInstance();
    metricDAO = daoRegistry.getMetricConfigDAO();
    datasetDAO = daoRegistry.getDatasetConfigDAO();
    eventDAO = daoRegistry.getEventDAO();
    anomalyDAO = daoRegistry.getMergedAnomalyResultDAO();
    evaluationDAO = daoRegistry.getEvaluationManager();
    detectionPipelineLoader = new DetectionPipelineLoader();

    Assert.assertNotNull(daoRegistry.getJobDAO());
  }

  @AfterClass(alwaysRun = true)
  void afterClass() throws Exception {
    cleanup_schedulers();
    testDAOProvider.cleanup();
  }

  private void cleanup_schedulers() throws SchedulerException {
    if (detectionScheduler != null) {
      detectionScheduler.shutdown();
    }
    if (alertScheduler != null) {
      alertScheduler.shutdown();
    }
    if (monitorJobScheduler != null) {
      monitorJobScheduler.shutdown();
    }
    if (taskDriver != null) {
      taskDriver.shutdown();
    }
    if (dataCompletenessScheduler != null) {
      dataCompletenessScheduler.shutdown();
    }
    if (classificationJobScheduler != null) {
      classificationJobScheduler.shutdown();
    }
  }

  private void setup() throws Exception {
    metricDAO.save(getTestMetricConfig(collection, metric, null));
    datasetDAO.save(getTestDatasetConfig(collection));

    // Application config
    thirdeyeAnomalyConfig = new ThirdEyeAnomalyConfiguration();
    thirdeyeAnomalyConfig.setId(id);
    thirdeyeAnomalyConfig.setDashboardHost(dashboardHost);
    // Monitor config
    MonitorConfiguration monitorConfiguration = new MonitorConfiguration();
    monitorConfiguration.setDefaultRetentionDays(MonitorConstants.DEFAULT_RETENTION_DAYS);
    monitorConfiguration.setCompletedJobRetentionDays(MonitorConstants.DEFAULT_COMPLETED_JOB_RETENTION_DAYS);
    monitorConfiguration.setDetectionStatusRetentionDays(MonitorConstants.DEFAULT_DETECTION_STATUS_RETENTION_DAYS);
    monitorConfiguration.setRawAnomalyRetentionDays(MonitorConstants.DEFAULT_RAW_ANOMALY_RETENTION_DAYS);
    monitorConfiguration.setMonitorFrequency(new TimeGranularity(3, TimeUnit.SECONDS));
    thirdeyeAnomalyConfig.setMonitorConfiguration(monitorConfiguration);
    // Task config
    TaskDriverConfiguration taskDriverConfiguration = new TaskDriverConfiguration();
    taskDriverConfiguration.setNoTaskDelayInMillis(1000);
    taskDriverConfiguration.setRandomDelayCapInMillis(200);
    taskDriverConfiguration.setTaskFailureDelayInMillis(500);
    taskDriverConfiguration.setMaxParallelTasks(2);
    thirdeyeAnomalyConfig.setTaskDriverConfiguration(taskDriverConfiguration);
    thirdeyeAnomalyConfig.setRootDir(System.getProperty("dw.rootDir", "NOT_SET(dw.rootDir)"));

    TimeSeriesLoader timeseriesLoader =
        new DefaultTimeSeriesLoader(daoRegistry.getMetricConfigDAO(), datasetDAO, null, null);
    AggregationLoader aggregationLoader =
        new DefaultAggregationLoader(metricDAO, datasetDAO, ThirdEyeCacheRegistry.getInstance().getQueryCache(),
            ThirdEyeCacheRegistry.getInstance().getDatasetMaxDataTimeCache());

    DataProvider provider = new DefaultDataProvider(metricDAO, datasetDAO, eventDAO, anomalyDAO, evaluationDAO,
        timeseriesLoader, aggregationLoader, detectionPipelineLoader);

    // create test detection
    functionId = daoRegistry.getDetectionConfigManager().save(DaoTestUtils.getTestDetectionConfig(provider, detectionConfigFile));

    // create test subscription
    daoRegistry.getDetectionAlertConfigManager().save(getTestDetectionAlertConfig(daoRegistry.getDetectionConfigManager(), alertConfigFile));
  }

  @Test
  public void testThirdeyeAnomalyApplication() throws Exception {

    Assert.assertNotNull(daoRegistry.getJobDAO());

    setup();

    Assert.assertNotNull(daoRegistry.getJobDAO());

    TaskManager taskDAO = daoRegistry.getTaskDAO();

    // startDataCompletenessChecker
    startDataCompletenessScheduler();
    Thread.sleep(1000);
    int jobSizeDataCompleteness = daoRegistry.getJobDAO().findAll().size();
    int taskSizeDataCompleteness = taskDAO.findAll().size();
    Assert.assertEquals(jobSizeDataCompleteness, 1);
    Assert.assertEquals(taskSizeDataCompleteness, 2);
    JobDTO jobDTO = daoRegistry.getJobDAO().findAll().get(0);
    Assert.assertTrue(jobDTO.getJobName().startsWith(TaskType.DATA_COMPLETENESS.toString()));
    List<TaskDTO> taskDTOs = taskDAO.findAll();
    for (TaskDTO taskDTO : taskDTOs) {
      Assert.assertEquals(taskDTO.getTaskType(), TaskType.DATA_COMPLETENESS);
      Assert.assertEquals(taskDTO.getStatus(), TaskStatus.WAITING);
      DataCompletenessTaskInfo taskInfo = (DataCompletenessTaskInfo) TaskInfoFactory.
          getTaskInfoFromTaskType(taskDTO.getTaskType(), taskDTO.getTaskInfo());
      Assert.assertTrue((taskInfo.getDataCompletenessType() == DataCompletenessType.CHECKER)
          || (taskInfo.getDataCompletenessType() == DataCompletenessType.CLEANUP));
    }

    // start detection scheduler
    startDetectionScheduler();

    // start alert scheduler
    startAlertScheduler();

    // check for number of entries in tasks and jobs
    Thread.sleep(1000);
    int jobSize1 = daoRegistry.getJobDAO().findAll().size();
    int taskSize1 = taskDAO.findAll().size();
    Assert.assertTrue(jobSize1 > 0);
    Assert.assertTrue(taskSize1 > 0);
    Thread.sleep(10000);
    int taskSize2 = taskDAO.findAll().size();
    Assert.assertTrue(taskSize2 > taskSize1);

    tasks = taskDAO.findAll();

    // check for task type
    int detectionCount = 0;
    int subsCount = 0;
    for (TaskDTO task : tasks) {
      if (task.getTaskType().equals(TaskType.DETECTION)) {
        detectionCount ++;
      } else if (task.getTaskType().equals(TaskType.DETECTION_ALERT)) {
        subsCount ++;
      }
    }
    Assert.assertTrue(detectionCount > 0);
    Assert.assertTrue(subsCount > 0);

    // check for task status
    tasks = taskDAO.findAll();
    for (TaskDTO task : tasks) {
      Assert.assertEquals(task.getStatus(), TaskStatus.WAITING);
    }

    // start monitor
    startMonitor();

    // check for monitor tasks
    Thread.sleep(5000);
    tasks = taskDAO.findAll();
    int monitorCount = 0;
    for (TaskDTO task : tasks) {
      if (task.getTaskType().equals(TaskType.MONITOR)) {
        monitorCount++;
      }
    }
    Assert.assertTrue(monitorCount > 0);

    // check for job status
    jobs = daoRegistry.getJobDAO().findAll();
    for (JobDTO job : jobs) {
      Assert.assertEquals(job.getStatus(), JobStatus.SCHEDULED);
    }

    // start task drivers
    startWorker();

    // check for change in task status to COMPLETED
    Thread.sleep(30000);
    tasks = taskDAO.findAll();
    int completedCount = 0;
    for (TaskDTO task : tasks) {
      if (task.getStatus().equals(TaskStatus.COMPLETED)) {
        completedCount++;
      }
    }
    Assert.assertTrue(completedCount > 0);

    // check merged anomalies
    List<MergedAnomalyResultDTO> mergedAnomalies = daoRegistry.getMergedAnomalyResultDAO().findByFunctionId(functionId);
    Assert.assertTrue(mergedAnomalies.size() > 0);
    AnomalyFunctionDTO functionSpec = daoRegistry.getAnomalyFunctionDAO().findById(functionId);
    for (MergedAnomalyResultDTO mergedAnomaly : mergedAnomalies) {
      Assert.assertEquals(mergedAnomaly.getFunction(), functionSpec);
      Assert.assertEquals(mergedAnomaly.getCollection(), functionSpec.getCollection());
      Assert.assertEquals(mergedAnomaly.getMetric(), functionSpec.getTopicMetric());
      Assert.assertNotNull(mergedAnomaly.getAnomalyResultSource());
      Assert.assertNotNull(mergedAnomaly.getDimensions());
      Assert.assertNotNull(mergedAnomaly.getProperties());
      Assert.assertNull(mergedAnomaly.getFeedback());
    }

    // THE FOLLOWING TEST MAY FAIL OCCASIONALLY DUE TO MACHINE COMPUTATION POWER
    // check for job status COMPLETED
    jobs = daoRegistry.getJobDAO().findAll();
    int completedJobCount = 0;
    for (JobDTO job : jobs) {
      int attempt = 0;
      while (attempt < 3 && !job.getStatus().equals(JobStatus.COMPLETED)) {
        LOG.info("Checking job status with attempt : {}", attempt + 1);
        Thread.sleep(5_000);
        attempt++;
      }
      if (job.getStatus().equals(JobStatus.COMPLETED)) {
        completedJobCount++;
        break;
      }
    }
    Assert.assertTrue(completedJobCount > 0);

    // start classifier
    startClassifier();
    List<JobDTO> latestCompletedDetectionJobDTO =
        daoRegistry.getJobDAO().findRecentScheduledJobByTypeAndConfigId(TaskType.ANOMALY_DETECTION, functionId, 0L);
    Assert.assertNotNull(latestCompletedDetectionJobDTO);
    Assert.assertEquals(latestCompletedDetectionJobDTO.get(0).getStatus(), JobStatus.COMPLETED);
    Thread.sleep(5000);
    jobs = daoRegistry.getJobDAO().findAll();
  }

  private void startDataCompletenessScheduler() throws Exception {
    dataCompletenessScheduler = new DataCompletenessScheduler();
    dataCompletenessScheduler.start();
  }

  private void startClassifier() {
    classificationJobScheduler = new ClassificationJobScheduler();
    classificationJobScheduler.start();
  }

  private void startMonitor() {
    monitorJobScheduler = new MonitorJobScheduler(thirdeyeAnomalyConfig.getMonitorConfiguration());
    monitorJobScheduler.start();
  }


  private void startWorker() throws Exception {
    taskDriver =
        new TaskDriver(thirdeyeAnomalyConfig, anomalyFunctionFactory, alertFilterFactory, anomalyClassifierFactory);
    taskDriver.start();
  }

  private void startAlertScheduler() throws Exception {
    alertScheduler = new DetectionAlertScheduler();
    alertScheduler.start();
  }


  private void startDetectionScheduler() throws Exception {
    detectionScheduler = new DetectionPipelineScheduler(DAORegistry.getInstance().getDetectionConfigManager());
    detectionScheduler.start();
  }
}