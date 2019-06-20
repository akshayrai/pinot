/*
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

package org.apache.pinot.thirdeye.detection.components;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.detection.DetectionTestUtils;
import org.apache.pinot.thirdeye.detection.spec.TriggerConditionGrouperSpec;
import org.apache.pinot.thirdeye.detection.spi.exception.DetectorException;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.apache.pinot.thirdeye.detection.components.TriggerConditionGrouper.*;


public class TriggerConditionGrouperTest {

  @BeforeMethod
  public void beforeMethod() {

  }

  public static MergedAnomalyResultDTO makeAnomaly(long start, long end, String entity) {
    MergedAnomalyResultDTO anomaly = DetectionTestUtils.makeAnomaly(1000l, start, end, null, null, Collections.<String, String>emptyMap());
    Map<String, String> props = new HashMap<>();
    props.put(PROP_DETECTOR_COMPONENT_NAME, entity);
    anomaly.setProperties(props);
    return anomaly;
  }


  @Test
  public void testAndGrouping() throws DetectorException {
    TriggerConditionGrouper grouper = new TriggerConditionGrouper();

    List<MergedAnomalyResultDTO> anomalies = new ArrayList<>();
    anomalies.add(makeAnomaly(0, 1000, "entityA"));
    anomalies.add(makeAnomaly(500, 2000, "entityB"));

    TriggerConditionGrouperSpec spec = new TriggerConditionGrouperSpec();
    spec.setOperator("and");
    Map<String, Object> leftOp = new HashMap<>();
    leftOp.put("value", "entityA");
    spec.setLeftOp(leftOp);

    Map<String, Object> rigthOp = new HashMap<>();
    rigthOp.put("value", "entityB");
    spec.setRightOp(rigthOp);

    grouper.init(spec, null);
    List<MergedAnomalyResultDTO> groupedAnomalies = grouper.group(anomalies);

    Assert.assertEquals(groupedAnomalies.size(), 1 + anomalies.size());

    int childCounter = 0;
    int parentCounter = 0;
    for (MergedAnomalyResultDTO anomaly : groupedAnomalies) {
      if (anomaly.isChild()) {
        childCounter++;
      } else {
        parentCounter++;
        Assert.assertEquals(anomaly.getStartTime(), 500);
        Assert.assertEquals(anomaly.getEndTime(), 1000);
        Assert.assertEquals(anomaly.getChildren().size(), 2);
      }
    }
    Assert.assertEquals(childCounter, 2);
    Assert.assertEquals(parentCounter, 1);
  }

  @Test
  public void testOrGrouping() throws DetectorException {
  }

  @Test
  public void testNestedGrouping() throws DetectorException {
  }
}