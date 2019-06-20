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
package org.apache.pinot.thirdeye.detection.components;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.collections4.MapUtils;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.detection.ConfigUtils;
import org.apache.pinot.thirdeye.detection.InputDataFetcher;
import org.apache.pinot.thirdeye.detection.annotation.Components;
import org.apache.pinot.thirdeye.detection.annotation.DetectionTag;
import org.apache.pinot.thirdeye.detection.annotation.Param;
import org.apache.pinot.thirdeye.detection.annotation.PresentationOption;
import org.apache.pinot.thirdeye.detection.spec.TriggerConditionGrouperSpec;
import org.apache.pinot.thirdeye.detection.spi.components.Grouper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Components(title = "TriggerCondition", type = "",
    tags = {DetectionTag.GROUPER}, description = "",
    presentation = {@PresentationOption(name = "group param value", template = "group by ${mockParam}")},
    params = {@Param(name = "mockParam", placeholder = "value")})
public class TriggerConditionGrouper implements Grouper<TriggerConditionGrouperSpec> {
  protected static final Logger LOG = LoggerFactory.getLogger(TriggerConditionGrouper.class);

  private String expression;
  private String operator;
  private Map<String, Object> leftOp;
  private Map<String, Object> rightOp;
  private InputDataFetcher dataFetcher;

  private static final String PROP_DETECTOR_COMPONENT_NAME = "detectorComponentName";
  private static final String PROP_AND = "and";
  private static final String PROP_OR = "or";
  private static final String PROP_OPERATOR = "operator";
  private static final String PROP_LEFT_OP = "leftOp";
  private static final String PROP_RIGHT_OP = "rightOp";

  private List<MergedAnomalyResultDTO> andGrouping(
      List<MergedAnomalyResultDTO> anomaliesA, List<MergedAnomalyResultDTO> anomaliesB) {
    List<MergedAnomalyResultDTO> groupedAnomalies = new ArrayList<>();




    // Mark children
    for (MergedAnomalyResultDTO anomaly : anomalies) {
      anomaly.setChild(true);
    }

    // Create Grouped Anomaly
    MergedAnomalyResultDTO groupedAnomaly = new MergedAnomalyResultDTO();
    //groupedAnomaly.setType();
    groupedAnomaly.setStartTime();
    groupedAnomaly.setEndTime();
    Map<String, String> properties = new HashMap<>();
    groupedAnomaly.setProperties();
    groupedAnomaly.setChildren((Set<MergedAnomalyResultDTO>) anomalies);
    groupedAnomaly.setChild(false);

    // Store all anomalies
    groupedAnomalies.add(groupedAnomaly);
    groupedAnomalies.addAll(anomalies);

    return groupedAnomalies;
  }

  private List<MergedAnomalyResultDTO> orGrouping(
      List<MergedAnomalyResultDTO> entityA, List<MergedAnomalyResultDTO> entityB) {
    List<MergedAnomalyResultDTO> groupedAnomalies = new ArrayList<>();
    // Filter anomalies from entityA
    // Filter anomalies from entityB
    // Perform merge and return
    return groupedAnomalies;
  }

  private List<MergedAnomalyResultDTO> executeChildExpression(Map<String, Object> op, List<MergedAnomalyResultDTO> anomalies) {
    Preconditions.checkNotNull(op);

    String value = MapUtils.getString(op, "value");
    if (value != null) {
      //filter and return anomalies;
      List<MergedAnomalyResultDTO> filteredAnomalies = new ArrayList<>();
      for (MergedAnomalyResultDTO anomaly : anomalies) {
        if (anomaly.getProperties() != null && anomaly.getProperties().get(PROP_DETECTOR_COMPONENT_NAME) != null
            && anomaly.getProperties().get(PROP_DETECTOR_COMPONENT_NAME).startsWith(value)) {
          filteredAnomalies.add(anomaly);
        }
      }

      return filteredAnomalies;
    }

    String operator = MapUtils.getString(op, PROP_OPERATOR);
    Map<String, Object> leftOp = ConfigUtils.getMap(op.get(PROP_LEFT_OP));
    Map<String, Object> rightOp = ConfigUtils.getMap(op.get(PROP_RIGHT_OP));
    return groupAnomaliesByOperator(operator, leftOp, rightOp, anomalies);
  }

  private List<MergedAnomalyResultDTO> groupAnomaliesByOperator(String operator,
      Map<String, Object> leftOp, Map<String, Object> rightOp, List<MergedAnomalyResultDTO> anomalies) {
    Preconditions.checkNotNull(operator);
    Preconditions.checkNotNull(leftOp);
    Preconditions.checkNotNull(rightOp);

    List<MergedAnomalyResultDTO> leftAnomalies = executeChildExpression(leftOp, anomalies);
    List<MergedAnomalyResultDTO> rightAnomalies = executeChildExpression(rightOp, anomalies);

    if (operator.equalsIgnoreCase(PROP_AND)) {
      return andGrouping(leftAnomalies, rightAnomalies);
    } else if (operator.equalsIgnoreCase(PROP_OR)) {
      return orGrouping(leftAnomalies, rightAnomalies);
    } else {
      throw new RuntimeException("Unsupported operator");
    }
  }

  // TODO: Build parse tree from string expression and execute
  private Map<String, Object> buildOperatorTree(String expression) {
    return new HashMap<>();
  }

  private List<MergedAnomalyResultDTO> groupAnomaliesByExpression(String expression, List<MergedAnomalyResultDTO> anomalies) {
    Map<String, Object> operatorRoot = buildOperatorTree(expression);
    String operator = MapUtils.getString(operatorRoot, PROP_OPERATOR);
    Map<String, Object> leftOp = ConfigUtils.getMap(operatorRoot.get(PROP_LEFT_OP));
    Map<String, Object> rightOp = ConfigUtils.getMap(operatorRoot.get(PROP_RIGHT_OP));
    groupAnomaliesByOperator(operator, leftOp, rightOp, anomalies);
    return anomalies;
  }

  @Override
  public List<MergedAnomalyResultDTO> group(List<MergedAnomalyResultDTO> anomalies) {
    if (operator != null) {
      return groupAnomaliesByOperator(operator, leftOp, rightOp, anomalies);
    } else {
      return groupAnomaliesByExpression(expression, anomalies);
    }
  }

  @Override
  public void init(TriggerConditionGrouperSpec spec, InputDataFetcher dataFetcher) {
    this.expression = spec.getExpression();
    this.operator = spec.getOperator();
    this.leftOp = spec.getLeftOp();
    this.rightOp = spec.getRightOp();
    this.dataFetcher = dataFetcher;
  }
}
