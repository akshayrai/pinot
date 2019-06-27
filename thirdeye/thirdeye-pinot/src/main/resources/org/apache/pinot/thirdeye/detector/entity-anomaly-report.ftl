<#import "lib/utils.ftl" as utils>

<head>
  <link href="https://fonts.googleapis.com/css?family=Open+Sans" rel="stylesheet">
</head>

<body style="background-color: #EDF0F3;">
<table border="0" cellpadding="0" cellspacing="0" style="width:100%; font-family: 'Proxima Nova','Arial','Helvetica Neue',Helvetica,sans-serif; font-size:14px; margin:0 auto; max-width: 50%; min-width: 700px; background-color: #F3F6F8;">
  <!-- White bar on top -->
  <tr>
    <td align="center" style="padding: 6px 24px;">
      <span style="color: rgba(0,0,0,0.75); font-size: 18px; font-weight: bold; letter-spacing: 2px; vertical-align: middle;">THIRDEYE ALERT</span>
    </td>
    <p>Anomaly details</p>
  </tr>

  <!-- Blue header on top -->
  <tr>
    <td style="font-size: 16px; padding: 12px; background-color: #0073B1; color: #FFF; text-align: center;">
      <span style="font-size: 18px; font-weight: bold; letter-spacing: 2px; vertical-align: middle;">Anomaly Report</span>
    </td>
  </tr>

  <tr>
    <td>
      <table border="0" cellpadding="0" cellspacing="0" style="border:1px solid #E9E9E9; border-radius: 2px; width: 100%;">

        <!-- List all the alerts -->
        <#list entityToAnomalyDetailsMap?keys as entity>
          <@utils.addBlock title="" align="left">
            <!-- Display Entity Name -->
            <p>
              <span style="color: #1D1D1D; font-size: 20px; font-weight: bold; display:inline-block; vertical-align: middle;">Entity:&nbsp;</span>
              <span style="color: #606060; font-size: 20px; text-decoration: none; display:inline-block; vertical-align: middle; width: 70%; white-space: nowrap; overflow: hidden; text-overflow: ellipsis;">${entity}</span>
            </p>

            <!-- List down all the alerts for the given entity -->
            <#list detectionToAnomalyDetailsMap?keys as detectionName>
              <#assign newTable = false>
              <#list detectionToAnomalyDetailsMap[detectionName] as anomaly>
                <#if anomaly.metric==metric>
                  <#assign newTable=true>
                  <#assign description=anomaly.funcDescription>
                </#if>
              </#list>

              <!-- Alert along with description -->
              <#if newTable>
                <p>
                  <span style="color: #1D1D1D; font-size: 16px; font-weight: bold; display:inline-block; vertical-align: middle;">Alert:&nbsp;</span>
                  <span style="color: #606060; font-size: 16px; text-decoration: none; display:inline-block; vertical-align: middle; width: 77%; white-space: nowrap; overflow: hidden; text-overflow: ellipsis;">${detectionName}</span>
                  <a href="${dashboardHost}/app/#/manage/explore/${functionToId[detectionName]?string.computer}" target="blank" style="text-decoration: none; color: #0B5EA1; display:inline-block; vertical-align: middle;">(Edit Settings)</a>
                </p>
                <p>
                  <span style="color: #606060; font-size: 13px; text-decoration: none; display:inline-block; vertical-align: middle; width: 77%; white-space: wrap;">${description}</span>
                </p>
              </#if>

              <!-- List all the anomalies under this detection -->
              <table border="0" width="100%" align="center" style="width:100%; padding:0; margin:0; border-collapse: collapse;text-align:left;">
                <#list detectionToAnomalyDetailsMap[detectionName] as anomaly>
                  <#if anomaly.metric==metric>
                    <#if newTable>
                      <tr style="text-align:center; background-color: #F6F8FA; border-top: 2px solid #C7D1D8; border-bottom: 2px solid #C7D1D8;">
                        <th style="text-align:left; padding: 6px 12px; font-size: 12px; font-weight: bold; line-height: 20px;">Feature</th>
                        <th style="padding: 6px 12px; font-size: 12px; font-weight: bold; line-height: 20px;">Criticality Score</th>
                        <th style="padding: 6px 12px; font-size: 12px; font-weight: bold; line-height: 20px;">Avg. % of change</th>
                        <th style="padding: 6px 12px; font-size: 12px; font-weight: bold; line-height: 20px;">Current</th>
                        <th style="padding: 6px 12px; font-size: 12px; font-weight: bold; line-height: 20px;">Predicted</th>
                      </tr>
                    </#if>
                    <#assign newTable = false>
                    <tr style="border-bottom: 1px solid #C7D1D8;">
                      <td style="padding: 6px 12px;white-space: nowrap;">
                        <div style="color: rgba(0,0,0,0.9); font-size:14px; line-height:20px;">${anomaly.entityName}</div>
                        <span style="color: rgba(0,0,0,0.6); font-size:12px; line-height:16px;">${anomaly.duration}</span>
                        <a style="font-weight: bold; text-decoration: none; font-size:14px; line-height:20px; color: #0073B1;" href="${anomaly.anomalyURL}${anomaly.anomalyId}"
                           target="_blank">(view)</a>
                      </td>
                      <td style="color: rgba(0,0,0,0.9); font-size:14px; line-height:20px; text-align:center;">${anomaly.score}</td>
                      <td style="color: rgba(0,0,0,0.9); font-size:14px; line-height:20px; text-align:center;">${anomaly.weight}</td>
                      <td style="color: rgba(0,0,0,0.9); font-size:14px; line-height:20px; text-align:center;">${anomaly.currentVal}</td>
                      <td style="color: rgba(0,0,0,0.9); font-size:14px; line-height:20px; text-align:center;">
                        ${anomaly.baselineVal}
                        <div style="font-size: 12px; color:${anomaly.positiveLift?string('#3A8C18','#ee1620')};">(${anomaly.positiveLift?string('+','')}${anomaly.lift})</div>
                      </td>
                    </tr>
                  </#if>
                </#list>
              </table>
            </#list>

          </@utils.addBlock>
        </#list>

        <!-- Reference Links -->
        <#if referenceLinks?has_content>
          <@utils.addBlock title="Useful Links" align="left">
            <table border="0" align="center" style="table-layout: fixed; width:100%; padding:0; margin:0; border-collapse: collapse; text-align:left;">
              <#list referenceLinks?keys as referenceLinkKey>
                <tr style="border-bottom: 1px solid #C7D1D8; padding: 16px;">
                  <td style="padding: 6px 12px;">
                    <a href="${referenceLinks[referenceLinkKey]}" style="text-decoration: none; color:#0073B1; font-size:12px; font-weight:bold;">${referenceLinkKey}</a>
                  </td>
                </tr>
              </#list>
            </table>
          </@utils.addBlock>
        </#if>

      </table>
    </td>
  </tr>

  <tr>
    <td style="text-align: center; background-color: #EDF0F3; font-size: 12px; font-family:'Proxima Nova','Arial', 'Helvetica Neue',Helvetica, sans-serif; color: #737373; padding: 12px;">
      <p style="margin-top:0;"> You are receiving this email because you have subscribed to ThirdEye Alert Service for
        <strong>${alertConfigName}</strong>.</p>
      <p>
        If you have any questions regarding this report, please email
        <a style="color: #33aada;" href="mailto:ask_thirdeye@linkedin.com" target="_top">ask_thirdeye@linkedin.com</a>
      </p>
    </td>
  </tr>

</table>
</body>
