package com.linkedin.thirdeye.detection.yaml;

import com.google.common.base.CaseFormat;
import com.linkedin.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import com.linkedin.thirdeye.datalayer.pojo.AlertConfigBean;
import com.linkedin.thirdeye.detection.ConfigUtils;
import com.linkedin.thirdeye.detection.annotation.DetectionRegistry;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.commons.collections.MapUtils;


/**
 * The translator converts the alert yaml config into a detection alert config
 * TODO Refactor this to support alert schemes
 */
public class YamlDetectionAlertConfigTranslator {
  private static final String PROP_SUBS_GROUP_NAME = "subscriptionGroupName";
  private static final String PROP_CRON = "cron";
  private static final String PROP_ACTIVE = "cron";
  private static final String PROP_APPLICATION = "application";
  private static final String PROP_FROM = "fromAddress";
  private static final String PROP_ONLY_FETCH_LEGACY_ANOMALIES = "onlyFetchLegacyAnomalies";
  private static final String PROP_EMAIL_SUBJECT_TYPE = "emailSubjectStyle";
  private static final String PROP_TYPE = "type";
  private static final String PROP_CLASS_NAME = "className";
  private static final String PROP_DETECTION_CONFIG_IDS = "detectionConfigIds";
  private static final String PROP_ALERT_SCHEMES = "alertSchemes";
  private static final String PROP_ALERT_SUPPRESSORS = "alertSuppressors";
  private static final String PROP_PARAM = "params";
  private static final String PROP_REFERENCE_LINKS = "referenceLinks";
  private static final String PROP_RECIPIENTS = "recipients";
  private static final String PROP_DIMENSION = "dimension";
  private static final String PROP_DIMENSION_RECIPIENTS = "dimensionRecipients";

  private Map<String,Object> yamlAlertConfig;

  private static final String DEFAULT_CRON_SCHEDULE = "0 0/5 * * * ? *"; // Every 5 min
  private static final DetectionRegistry DETECTION_REGISTRY = DetectionRegistry.getInstance();
  private static final Set<String> PROPERTY_KEYS = new HashSet<>(
      Arrays.asList(PROP_DETECTION_CONFIG_IDS, PROP_RECIPIENTS, PROP_DIMENSION, PROP_DIMENSION_RECIPIENTS));

  public YamlDetectionAlertConfigTranslator(Map<String,Object> newAlertConfig) {
    this.yamlAlertConfig = newAlertConfig;
  }

  private Map<String, Object> buildAlerterProperties(Map<String, Object> alertYamlConfigs) {
    Map<String, Object> properties = new HashMap<>();
    for (Map.Entry<String, Object> entry : alertYamlConfigs.entrySet()) {
      if (entry.getKey().equals(PROP_TYPE)) {
        properties.put(PROP_CLASS_NAME, DETECTION_REGISTRY.lookup(MapUtils.getString(alertYamlConfigs, PROP_TYPE)));
      } else {
        if (PROPERTY_KEYS.contains(entry.getKey())) {
          properties.put(entry.getKey(), entry.getValue());
        }
      }
    }

    return properties;
  }

  @SuppressWarnings("unchecked")
  private Map<String,Map<String,Object>> buildAlertSuppressors(Map<String,Object> yamlAlertConfig) {
    List<Map<String, Object>> alertSuppressors = ConfigUtils.getList(yamlAlertConfig.get(PROP_ALERT_SUPPRESSORS));
    Map<String, Map<String, Object>> alertSuppressorsHolder = new HashMap<>();
    Map<String, Object> alertSuppressorsParsed = new HashMap<>();
    if (!alertSuppressors.isEmpty()) {
      for (int i = 0; i <= alertSuppressors.size(); i++) {
        Map<String, Object> alertSuppressor = alertSuppressors.get(i);
        if (alertSuppressor.get(PROP_PARAM) == null) {
          Map<String, Object> params = new HashMap<>();
          alertSuppressor.put(PROP_PARAM, params);
        }
        if (alertSuppressor.get(PROP_TYPE) != null) {
          ((Map<String, Object>) alertSuppressor.get(PROP_PARAM))
              .put(PROP_CLASS_NAME, DETECTION_REGISTRY.lookupAlertSuppressors(alertSuppressor.get(PROP_TYPE).toString()));
        }

        String suppressorType = CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, alertSuppressor.get(PROP_TYPE).toString());
        alertSuppressorsParsed.put(suppressorType + "Suppressor", alertSuppressor.get(PROP_PARAM));
      }
    }
    alertSuppressorsHolder.put(PROP_ALERT_SUPPRESSORS, alertSuppressorsParsed);

    return alertSuppressorsHolder;
  }

  @SuppressWarnings("unchecked")
  private Map<String,Map<String,Object>> buildAlertSchemes(Map<String,Object> yamlAlertConfig) {
    List<Map<String, Object>> alertSchemes = ConfigUtils.getList(yamlAlertConfig.get(PROP_ALERT_SCHEMES));
    Map<String, Map<String, Object>> alertSchemesHolder = new HashMap<>();
    Map<String, Object> alertSchemesParsed = new HashMap<>();
    if (!alertSchemes.isEmpty()) {
      for (int i = 0; i <= alertSchemes.size(); i++) {
        Map<String, Object> alertScheme = alertSchemes.get(i);
        if (alertScheme.get(PROP_PARAM) == null) {
          Map<String, Object> params = new HashMap<>();
          alertScheme.put(PROP_PARAM, params);
        }
        if (alertScheme.get(PROP_TYPE) != null) {
          ((Map<String, Object>) alertScheme.get(PROP_PARAM))
              .put(PROP_CLASS_NAME, DETECTION_REGISTRY.lookupAlertSchemes(alertScheme.get(PROP_TYPE).toString()));
        }
        alertSchemesParsed.put(alertScheme.get(PROP_TYPE).toString().toLowerCase() + "Scheme", alertScheme.get(PROP_PARAM));
      }
    }
    alertSchemesHolder.put(PROP_ALERT_SCHEMES, alertSchemesParsed);

    return alertSchemesHolder;
  }

  /**
   * Generates the {@link DetectionAlertConfigDTO} from the YAML Alert Map
   */
  @SuppressWarnings("unchecked")
  public DetectionAlertConfigDTO translate() {
    DetectionAlertConfigDTO alertConfigDTO = new DetectionAlertConfigDTO();

    alertConfigDTO.setName(MapUtils.getString(yamlAlertConfig, PROP_SUBS_GROUP_NAME));
    alertConfigDTO.setApplication(MapUtils.getString(yamlAlertConfig, PROP_APPLICATION));
    alertConfigDTO.setFrom(MapUtils.getString(yamlAlertConfig, PROP_FROM));

    alertConfigDTO.setCronExpression(MapUtils.getString(yamlAlertConfig, PROP_CRON, DEFAULT_CRON_SCHEDULE));
    alertConfigDTO.setActive(MapUtils.getBooleanValue(yamlAlertConfig, PROP_ACTIVE, true));
    alertConfigDTO.setOnlyFetchLegacyAnomalies(MapUtils.getBooleanValue(yamlAlertConfig, PROP_ONLY_FETCH_LEGACY_ANOMALIES, false));
    alertConfigDTO.setSubjectType((AlertConfigBean.SubjectType) MapUtils.getObject(yamlAlertConfig, PROP_EMAIL_SUBJECT_TYPE, AlertConfigBean.SubjectType.METRICS));

    MapUtils.getMap(yamlAlertConfig, PROP_REFERENCE_LINKS).put("ThirdEye User Guide", "https://go/thirdeyeuserguide");
    MapUtils.getMap(yamlAlertConfig, PROP_REFERENCE_LINKS).put("Add Reference Links", "https://go/thirdeyealertreflink");
    alertConfigDTO.setReferenceLinks(MapUtils.getMap(yamlAlertConfig, PROP_REFERENCE_LINKS));

    alertConfigDTO.setAlertSchemes(buildAlertSchemes(yamlAlertConfig));
    alertConfigDTO.setAlertSuppressors(buildAlertSuppressors(yamlAlertConfig));
    alertConfigDTO.setProperties(buildAlerterProperties(yamlAlertConfig));

    // NOTE: The below fields will/should be hidden from the YAML/UI. They will only be updated by the backend pipeline.
    List<Long> detectionConfigIds = ConfigUtils.getList(yamlAlertConfig.get(PROP_DETECTION_CONFIG_IDS));
    Map<Long, Long> vectorClocks = new HashMap<>();
    for (long detectionConfigId : detectionConfigIds) {
      vectorClocks.put(detectionConfigId, 0L);
    }
    alertConfigDTO.setHighWaterMark(0L);
    alertConfigDTO.setVectorClocks(vectorClocks);

    return alertConfigDTO;
  }
}
