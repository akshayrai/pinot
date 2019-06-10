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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
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
  private InputDataFetcher dataFetcher;

  private void groupByTime() {

  }

  @Override
  public List<MergedAnomalyResultDTO> group(List<MergedAnomalyResultDTO> anomalies) {
    List<MergedAnomalyResultDTO> groupedAnomalies = new ArrayList<>();
    if (anomalies.size() > 1000) {
      // TODO: evaluate threshold and impact
      LOG.warn("Lot of anomalies detected. Skipping grouping");
      return anomalies;
    }

    ListMultimap<String, String> multimap = ArrayListMultimap.create();

    for (MergedAnomalyResultDTO anomaly : anomalies) {

    }

    MergedAnomalyResultDTO groupedAnomaly = new MergedAnomalyResultDTO();
    //groupedAnomaly.setType();
    groupedAnomaly.setStartTime();
    groupedAnomaly.setEndTime();

    Map<String, String> properties = new HashMap<>();
    groupedAnomaly.setProperties();
    groupedAnomaly.setChildren((Set<MergedAnomalyResultDTO>) anomalies);
    groupedAnomaly.setChild(false);
    for (MergedAnomalyResultDTO anomaly : anomalies) {
      anomaly.setChild(true);
    }

    groupedAnomalies.add(groupedAnomaly);
    groupedAnomalies.addAll(anomalies);

    return groupedAnomalies;
  }

  @Override
  public void init(TriggerConditionGrouperSpec spec, InputDataFetcher dataFetcher) {
    this.expression = spec.getExpression();
    this.dataFetcher = dataFetcher;
  }
}
