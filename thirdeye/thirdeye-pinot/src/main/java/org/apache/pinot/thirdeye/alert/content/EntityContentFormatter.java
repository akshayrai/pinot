/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.pinot.thirdeye.alert.content;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.pinot.thirdeye.anomalydetection.context.AnomalyResult;
import org.apache.pinot.thirdeye.datalayer.bao.DetectionConfigManager;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.apache.pinot.thirdeye.util.ThirdEyeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This email formatter generates a report/alert from the grouped composite anomalies
 */
public class EntityContentFormatter extends BaseEmailContentFormatter{
  private static final Logger LOG = LoggerFactory.getLogger(EntityContentFormatter.class);

  public static final String EMAIL_TEMPLATE = "emailTemplate";

  public static final String DEFAULT_EMAIL_TEMPLATE = "entity-anomaly-report.ftl";
  public static final String PROP_ANOMALY_SCORE = "score";
  public static final String PROP_GROUP_KEY = "groupKey";

  private DetectionConfigManager configDAO = null;

  public EntityContentFormatter(){
  }

  @Override
  public void init(Properties properties, EmailContentFormatterConfiguration configuration) {
    super.init(properties, configuration);
    this.emailTemplate = properties.getProperty(EMAIL_TEMPLATE, DEFAULT_EMAIL_TEMPLATE);
    this.configDAO = DAORegistry.getInstance().getDetectionConfigManager();
  }

  @Override
  protected void updateTemplateDataByAnomalyResults(Map<String, Object> templateData,
      Collection<AnomalyResult> anomalies, EmailContentFormatterContext context) {
    Map<String, Long> functionToId = new HashMap<>();
    Multimap<String, AnomalyReportEntity> functionAnomalyReports = ArrayListMultimap.create();
    Multimap<String, AnomalyReportEntity> entityAnomalyReports = ArrayListMultimap.create();
    List<String> anomalyIds = new ArrayList<>();

    for (AnomalyResult anomalyResult : anomalies) {
      if (!(anomalyResult instanceof MergedAnomalyResultDTO)) {
        LOG.warn("Anomaly result {} isn't an instance of MergedAnomalyResultDTO. Skip from alert.", anomalyResult);
        continue;
      }
      MergedAnomalyResultDTO anomaly = (MergedAnomalyResultDTO) anomalyResult;

      String feedbackVal = getFeedbackValue(anomaly.getFeedback());

      String functionName = "Alerts";
      String funcDescription = "";
      Long id = -1L;

      if (anomaly.getFunction() != null){
        functionName = anomaly.getFunction().getFunctionName();
        id = anomaly.getFunction().getId();
      } else if ( anomaly.getDetectionConfigId() != null){
        DetectionConfigDTO config = this.configDAO.findById(anomaly.getDetectionConfigId());
        Preconditions.checkNotNull(config, String.format("Cannot find detection config %d", anomaly.getDetectionConfigId()));
        functionName = config.getName();
        funcDescription = config.getDescription();
        id = config.getId();
      }

      AnomalyReportEntity anomalyReport = new AnomalyReportEntity(String.valueOf(anomaly.getId()),
          getAnomalyURL(anomaly, emailContentFormatterConfiguration.getDashboardHost()),
          ThirdEyeUtils.getRoundedValue(anomaly.getAvgBaselineVal()),
          ThirdEyeUtils.getRoundedValue(anomaly.getAvgCurrentVal()),
          0d,
          null,
          getTimeDiffInHours(anomaly.getStartTime(), anomaly.getEndTime()), // duration
          feedbackVal,
          functionName,
          funcDescription,
          anomaly.getMetric(),
          getDateString(anomaly.getStartTime(), dateTimeZone),
          getDateString(anomaly.getEndTime(), dateTimeZone),
          getTimezoneString(dateTimeZone),
          getIssueType(anomaly),
          anomaly.getProperties().get(PROP_ANOMALY_SCORE),
          anomaly.getWeight(),
          anomaly.getProperties().get(PROP_GROUP_KEY)
      );

      // include notified alerts only in the email
      if (!includeSentAnomaliesOnly || anomaly.isNotified()) {
        anomalyIds.add(anomalyReport.getAnomalyId());
        functionAnomalyReports.put(functionName, anomalyReport);
        entityAnomalyReports.put(anomaly.getProperties().get("entityName"), anomalyReport);
        functionToId.put(functionName, id);
      }
    }


    templateData.put("anomalyIds", Joiner.on(",").join(anomalyIds));
    templateData.put("detectionToAnomalyDetailsMap", functionAnomalyReports.asMap());
    templateData.put("entityToAnomalyDetailsMap", entityAnomalyReports.asMap());
    templateData.put("functionToId", functionToId);
  }
}
