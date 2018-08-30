package com.linkedin.thirdeye.tools.migrate;

import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.testng.annotations.Test;


public class TestRegressionGaussianScanFunctionMigrater {
  private Map<String, String> defaultProperties = (new RegressianGaussianScanFunctionMigrater()).getDefaultProperties();

  @Test
  public void TestRegressionGaussianScanFunctionMigraterWithTrivialProperties() {
    Properties trivialProperties = new Properties();
    trivialProperties.put("no use", "no use");
    AnomalyFunctionDTO trivialPropertiesFunction = MigraterTestUtils.getAnomalyFunctionDTO("REGRESSION_GAUSSIAN_SCAN",
        1, TimeUnit.HOURS, trivialProperties);
    new RegressianGaussianScanFunctionMigrater().migrate(trivialPropertiesFunction);
    Properties properties = trivialPropertiesFunction.toProperties();

    Assert.assertEquals("ANOMALY_FUNCTION_WRAPPER", trivialPropertiesFunction.getType());
    Assert.assertFalse(properties.containsKey("no use"));
    for (Entry<String, String> entry : defaultProperties.entrySet()) {
      Assert.assertTrue(properties.containsKey(entry.getKey()));
      Assert.assertEquals(entry.getValue(), properties.getProperty(entry.getKey()));
    }
  }

  @Test
  public void TestRegressionGaussianScanFunctionMigraterWithSomeProperties() {
    Properties someProperties = new Properties();
    someProperties.put("continuumOffsetSize", "720");
    AnomalyFunctionDTO somePropertiesFunction = MigraterTestUtils.getAnomalyFunctionDTO("REGRESSION_GAUSSIAN_SCAN",
        1, TimeUnit.HOURS, someProperties);
    new RegressianGaussianScanFunctionMigrater().migrate(somePropertiesFunction);
    Properties properties = somePropertiesFunction.toProperties();

    Assert.assertEquals("ANOMALY_FUNCTION_WRAPPER", somePropertiesFunction.getType());
    for (Entry<String, String> entry : defaultProperties.entrySet()) {
      Assert.assertTrue(properties.containsKey(entry.getKey()));
      if (entry.getKey().equals("variables.continuumOffset")) {
        Assert.assertEquals("P30D", properties.getProperty(entry.getKey()));
      } else {
        Assert.assertEquals(String.format("Assert Error on property key %s", entry.getKey()),
            entry.getValue(), properties.getProperty(entry.getKey()));
      }
    }
  }
}
