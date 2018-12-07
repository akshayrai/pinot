package com.linkedin.thirdeye.detection.validators;

import com.linkedin.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import java.util.Map;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;

import static com.linkedin.thirdeye.detection.yaml.YamlResource.*;


public class DetectionAlertConfigValidator extends ConfigValidator {

  public DetectionAlertConfigValidator() {
  }

  /**
   * Perform basic validations on the detection alert yaml file
   */
  @SuppressWarnings("unchecked")
  public boolean validateBasicAlertYAML(String yamlConfig, Map<String, String> responseMessage) {
    if (!super.validateBasicYAML(yamlConfig, responseMessage)) {
      return false;
    }

    Map<String, Object> yamlConfigMap = (Map<String, Object>) YAML.load(yamlConfig);
    String subsGroupName = MapUtils.getString(yamlConfigMap, PROP_SUBS_GROUP_NAME);
    if (StringUtils.isEmpty(subsGroupName)) {
      responseMessage.put("message", "Subscription group name field cannot be left empty.");
      return false;
    }

    return true;
  }

  /**
   * Perform basic validation on the alert config like verifying if all the required fields are set
   */
  @SuppressWarnings("unchecked")
  public boolean validateAlertConfig(DetectionAlertConfigDTO alertConfig,  Map<String, String> responseMessage) {
    boolean isValid = true;

    // Check for all the required fields in the alert
    if (StringUtils.isEmpty(alertConfig.getName())) {
      isValid = false;
      responseMessage.put("message", "Subscription group name field cannot be left empty.");
    }
    if (StringUtils.isEmpty(alertConfig.getApplication())) {
      isValid = false;
      responseMessage.put("message", "Application field cannot be left empty");
    }
    if (StringUtils.isEmpty(alertConfig.getFrom())) {
      isValid = false;
      responseMessage.put("message", "From address field cannot be left empty");
    }

    // At least one alertScheme is required
    if (alertConfig.getAlertSchemes() == null || alertConfig.getAlertSchemes().get("alertSchemes") == null || alertConfig.getAlertSchemes().get("alertSchemes").get("emailScheme") == null) {
      isValid = false;
      responseMessage.put("message", "From address field cannot be left empty");
    }
    // detectionConfigIds must be specified



    return isValid;
  }

  @SuppressWarnings("unchecked")
  public boolean validateUpdatedAlertConfig(DetectionAlertConfigDTO updatedAlertConfig,
      DetectionAlertConfigDTO oldAlertConfig, Map<String, String> responseMessage) {
    boolean isValid = true;

    if (!validateAlertConfig(updatedAlertConfig, responseMessage)) {
      return false;
    }

    // TODO: Add more checks. Check if the updated fields are valid
    if (super.applicationDAO.findByName(updatedAlertConfig.getApplication()).size() == 0) {
      responseMessage.put("message", "Application name doesn't exist in our registry. Please use an existing"
          + " application name. You may search for registered applications from the ThirdEye dashboard or reach out"
          + " to ask_thirdeye if you wish to setup a new application.");
      return false;
    }
    //...

    return isValid;
  }
}
