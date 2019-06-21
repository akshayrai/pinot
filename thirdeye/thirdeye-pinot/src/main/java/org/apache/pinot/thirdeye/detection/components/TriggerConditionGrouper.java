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
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
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

  static final String PROP_DETECTOR_COMPONENT_NAME = "detectorComponentName";
  static final String PROP_AND = "and";
  static final String PROP_OR = "or";
  static final String PROP_OPERATOR = "operator";
  static final String PROP_LEFT_OP = "leftOp";
  static final String PROP_RIGHT_OP = "rightOp";

  private static final Comparator<MergedAnomalyResultDTO> COMPARATOR = new Comparator<MergedAnomalyResultDTO>() {
    @Override
    public int compare(MergedAnomalyResultDTO o1, MergedAnomalyResultDTO o2) {
      return Long.compare(o1.getStartTime(), o2.getStartTime());
    }
  };

  /**
   * Since the anomalies from the respective entities/metrics are merged
   * before calling the grouper, we do not have to deal with overlapping
   * anomalies within an entity/metric
   *
   * Core logic:
   * Sort anomalies by start time and iterate through the list
   * Make an entity copy of 1st anomaly and push to stack
   * For each anomaly following, compare with the one at stack top
   * 1. No overlap:
   *    If the anomaly doesn't overlap, pop stack, push entity copy of current anomaly
   * 2. Partial overlap:
   *    If partial overlap, update the entity anomaly at stack top and push an entity copy of current anomaly
   * 3. Full overlap:
   *    If full overlap, pop stack into temp, push entity copy of current anomaly followed by temp
   * After the last anomaly, the stack will have one extra anomaly which needs to be popped out.
   * Now the stack will have grouped/entity anomalies
   * Note the each time an entity copy is created, the children needs to be marked and set appropriately
   *
   */
  private List<MergedAnomalyResultDTO> andGrouping(
      List<MergedAnomalyResultDTO> anomalyListA, List<MergedAnomalyResultDTO> anomalyListB) {
    List<MergedAnomalyResultDTO> groupedAnomalies = new ArrayList<>();
    List<MergedAnomalyResultDTO> anomalies = new ArrayList<>();
    anomalies.addAll(anomalyListA);
    anomalies.addAll(anomalyListB);

    // Sort by increasing order of anomaly start time
    Collections.sort(anomalies, COMPARATOR);

    Stack<MergedAnomalyResultDTO> groupedAnomalyStack = new Stack<>();
    for (MergedAnomalyResultDTO anomaly : anomalies) {
      if (groupedAnomalyStack.isEmpty()) {
        groupedAnomalyStack.push(makeEntityAnomaly(anomaly));
        continue;
      }

      MergedAnomalyResultDTO anomalyStackTop = groupedAnomalyStack.peek();

      if (anomaly.getStartTime() > anomalyStackTop.getEndTime()) {
        // No overlap
        groupedAnomalyStack.pop();
        groupedAnomalyStack.push(makeEntityAnomaly(anomaly));
      } else if (anomaly.getEndTime() > anomalyStackTop.getEndTime()) {
        //Partial overlap
        anomalyStackTop.setStartTime(anomaly.getStartTime());
        anomalyStackTop.getChildren().add(anomaly);
        anomaly.setChild(true);
        groupedAnomalyStack.push(makeEntityAnomaly(anomaly));
      } else {
        // Complete overlap
        groupedAnomalyStack.pop();
        groupedAnomalyStack.push(makeEntityAnomaly(anomaly));
        groupedAnomalyStack.peek().getChildren().add(anomalyStackTop);
        groupedAnomalyStack.push(anomalyStackTop);
      }
    }

    // Pop the extra anomaly out
    if (!groupedAnomalyStack.isEmpty()) {
      groupedAnomalyStack.pop();
    }

    // Store all anomalies
    groupedAnomalies.addAll(groupedAnomalyStack);
    for (MergedAnomalyResultDTO anomaly : groupedAnomalyStack) {
      groupedAnomalies.addAll(anomaly.getChildren());
    }

    return groupedAnomalies;
  }

  private MergedAnomalyResultDTO makeEntityAnomaly(MergedAnomalyResultDTO anomaly) {
    MergedAnomalyResultDTO groupedAnomaly = new MergedAnomalyResultDTO();
    //groupedAnomaly.setType();
    groupedAnomaly.setStartTime(anomaly.getStartTime());
    groupedAnomaly.setEndTime(anomaly.getEndTime());
    //Map<String, String> properties = new HashMap<>();
    //groupedAnomaly.setProperties();
    groupedAnomaly.setChildren(new HashSet<>(Arrays.asList(anomaly)));
    groupedAnomaly.setChild(false);

    anomaly.setChild(true);

    return groupedAnomaly;
  }

  /**
   *
   * Sort anomalies by start time and iterate through the list
   * Make an entity copy of 1st anomaly and push to stack
   * For each anomaly following, compare with the one at stack top
   * 1. No overlap:
   *    If the anomaly doesn't overlap, push entity copy of current anomaly
   * 2. Partial overlap:
   *    If partial overlap, update stack top by increasing the end time
   * 3. Full overlap:
   *    If full overlap, ignore after update stack top with child info
   * Note the each time an entity copy is created, the children needs to be marked and set appropriately
   *
   */
  private List<MergedAnomalyResultDTO> orGrouping(
      List<MergedAnomalyResultDTO> anomalyListA, List<MergedAnomalyResultDTO> anomalyListB) {
    List<MergedAnomalyResultDTO> groupedAnomalies = new ArrayList<>();

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
