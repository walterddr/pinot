/**
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
package org.apache.pinot.tools;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.tools.admin.PinotAdministrator;
import org.apache.pinot.tools.admin.command.QuickstartRunner;


public class MultistageEngineQuickStart extends Quickstart {
  private static final String[] MULTI_STAGE_TABLE_DIRECTORIES = new String[]{
      "examples/batch/ssb/customer",
      "examples/batch/ssb/dates",
      "examples/batch/ssb/lineorder",
      "examples/batch/ssb/part",
      "examples/batch/ssb/supplier",
  };

  @Override
  protected int getNumQuickstartRunnerServers() {
    return 3;
  }

  @Override
  public List<String> types() {
    return Collections.singletonList("MULTI_STAGE");
  }

  @Override
  public void runSampleQueries(QuickstartRunner runner)
      throws Exception {

    printStatus(Quickstart.Color.YELLOW, "***** Multi-stage engine quickstart setup complete *****");
    Map<String, String> queryOptions = Collections.singletonMap("queryOptions",
        CommonConstants.Broker.Request.QueryOptionKey.USE_MULTISTAGE_ENGINE + "=true");
    String q1 = "SELECT count(*) FROM baseballStats_OFFLINE";
    printStatus(Quickstart.Color.YELLOW, "Total number of documents in the table");
    printStatus(Quickstart.Color.CYAN, "Query : " + q1);
    printStatus(Quickstart.Color.YELLOW, prettyPrintResponse(runner.runQuery(q1, queryOptions)));
    printStatus(Quickstart.Color.GREEN, "***************************************************");

    String q2 = "SELECT a.playerID, a.runs, a.yearID, b.runs, b.yearID"
        + " FROM baseballStats_OFFLINE AS a JOIN baseballStats_OFFLINE AS b ON a.playerID = b.playerID"
        + " WHERE a.runs > 160 AND b.runs < 2";
    printStatus(Quickstart.Color.YELLOW, "Correlate the same player(s) with more than 160-run some year(s) and"
        + " with less than 2-run some other year(s)");
    printStatus(Quickstart.Color.CYAN, "Query : " + q2);
    printStatus(Quickstart.Color.YELLOW, prettyPrintResponse(runner.runQuery(q2, queryOptions)));
    printStatus(Quickstart.Color.GREEN, "***************************************************");

    String q3 = "SELECT a.playerName, a.teamID, b.teamName \n"
        + "FROM baseballStats_OFFLINE AS a\n"
        + "JOIN dimBaseballTeams_OFFLINE AS b\n"
        + "ON a.teamID = b.teamID";
    printStatus(Quickstart.Color.YELLOW, "Baseball Stats with joined team names");
    printStatus(Quickstart.Color.CYAN, "Query : " + q3);
    printStatus(Quickstart.Color.YELLOW, prettyPrintResponse(runner.runQuery(q3, queryOptions)));
    printStatus(Quickstart.Color.GREEN, "***************************************************");

    printStatus(Quickstart.Color.GREEN, "***************************************************");
    printStatus(Quickstart.Color.YELLOW, "Example query run completed.");
    printStatus(Quickstart.Color.GREEN, "***************************************************");

//    String splitioQuery = """
//            WITH by_key AS
//              ( SELECT key,
//                       -- I use cityhash64 here because it's faster to work with fixed types and this intermediate node
//                       -- is only needed to exclude keys
//                       -- COUNT(DISTINCT rule) AS rules,
//                       -- COUNT(DISTINCT rule, treatment) AS rule_treatment,
//                       -- COUNT(DISTINCT rule, start_ts) AS ts_rule
//                       COUNT(DISTINCT rule) AS rules,
//                       COUNT(DISTINCT rule, treatment) AS rule_treatment,
//                       COUNT(DISTINCT rule, start_ts) AS ts_rule
//               FROM splitio_treatments
//               WHERE orgId = '4d3405a0-9ca5-11e5-9706-16a11fb02dec'
//                 AND testId = 'd6b439b0-5555-11ed-9633-6a8c054b7423'
//                 AND changeNumber = 1667515610690
//                 AND envId = '4fbab080-9ca5-11e5-9706-16a11fb02dec'
//               GROUP BY key
//            ),
//            excluded AS
//              (
//               SELECT key
//               FROM by_key
//               WHERE rules > 2  -- This is re-written in COUNT DISTINCT, but I AM NOT SURE I ENTIRELY FOLLOW THE LOGIC
//                 OR rule_treatment > 2
//                 OR length(rules) = 2
//                 -- this transforms
//                 -- (0, 'rule0'), (2, 'rule0'), (1, 'rule1')
//                 -- -> sort
//                 -- (0, 'rule0'), (1, 'rule1'), (2, 'rule0')
//                 -- -> compact
//                 -- 'rule0', 'rule1', 'rule0'
//                 -- most likely this need to be changed but works with 2 rules
//            ),
//            active_impressions AS (
//                SELECT key, rule, treatment, start_ts, end_ts
//                FROM splitio_treatments
//                WHERE
//                    orgId = '4d3405a0-9ca5-11e5-9706-16a11fb02dec'
//                    and testId = 'd6b439b0-5555-11ed-9633-6a8c054b7423'
//                    and changeNumber = 1667515610690
//                    and envId = '4fbab080-9ca5-11e5-9706-16a11fb02dec'
//                    and rule = 'default rule'
//                    and key NOT IN (
//                        select key
//                        from excluded
//                    )
//                    order by key, rule, treatment, start_ts
//            ),
//            treatments_generated AS (
//              select
//                  key,
//                  rule,
//                  treatment,
//                  min(start_ts) AS start_ts,
//                  max(end_ts) AS end_ts
//              from active_impressions
//              group by key, rule, treatment
//            ),
//            filter_events AS (
//                SELECT key, receptionTimestamp, value, eventTypeId
//                -- FROM events_split_test2
//                FROM splitio_events
//                WHERE
//                    orgId = '4d3405a0-9ca5-11e5-9706-16a11fb02dec'
//                    AND environmentId = '4fbab080-9ca5-11e5-9706-16a11fb02dec'
//                    AND key IN (
//                      SELECT key
//                      FROM active_impressions
//                    )
//                GROUP BY key, receptionTimestamp, value, eventTypeId
//            ),
//            joined_events_treatments AS (
//              SELECT t.key, value, treatment, rule, eventTypeId, receptionTimestamp\s
//                -- toDateTime(receptionTimestamp/1000.0) dateTime
//              FROM treatments_generated t LEFT JOIN filter_events e USING (key)
//              -- when we join to events, we care about events 15 minutes before start and 15 minutes after end
//              WHERE receptionTimestamp > start_ts - 900000\s
//                AND receptionTimestamp < least(coalesce(end_ts, 1668817941279) + 900000, 1668817941279)
//                                                                              -- 18446744073709551615)
//              ORDER BY receptionTimestamp asc
//            ),
//            event_type_and_rule_filter AS (
//                SELECT\s
//                  rule,
//                  treatment,
//                  key,
//                  value,
//                  eventTypeId,
//                  receptionTimestamp
//                FROM joined_events_treatments e
//                WHERE e.eventTypeId IN ('split.split.definition.timeSinceLastUpdate')
//                and value <> 0
//            ),
//            filter_qualified_keys AS (
//              SELECT key, min(receptionTimestamp) AS "timestamp"
//              from filter_events\s
//              where eventTypeId = 'split.split.definition.timeSinceLastUpdate'
//              GROUP BY key
//            ),
//            aggregate_keys AS (
//              SELECT
//                treatment,
//                rule,
//                f.key,
//                count(value) as attributed,
//                sum(value) as sum,
//                avg(value) as avg,
//                max(value) as max,
//                min(value) as min,
//                sum(case when eventTypeId =  '' then 1 else 0 end) as countMissing,
//                sum(case when eventTypeId != '' then 1 else 0 end) as countPresent
//              FROM event_type_and_rule_filter f
//              INNER JOIN filter_qualified_keys q
//              USING(key)
//              WHERE f.receptionTimestamp > q."timestamp" -- "has done before"
//                GROUP BY
//                rule,
//                treatment,
//                f.key
//              ORDER BY f.key
//            )
//            SELECT
//              a.treatment as Treatment,\s
//              count(a.key) as "Sample Size",
//              avg(avg) as Mean,
//              -- stddevSampStable(avg) as Stdev,
//              min(avg) as Min,
//              -- medianExact(avg) as Median,
//              max(avg) as Max,
//              -- quantiles(0.95, 0.99)(avg) as quantiles,
//              -- varSampStable(avg) as variance,
//              -- skewSamp(avg) as skew,
//              -- kurtSamp(avg) as kurtosis,
//              sum(countPresent) as eventsCount
//              FROM aggregate_keys a
//            group by a.treatment
//        """
//    printStatus(Quickstart.Color.YELLOW, "SplitIO query");
//    printStatus(Quickstart.Color.CYAN, "Query : " + splitioQuery);
//    printStatus(Quickstart.Color.YELLOW, prettyPrintResponse(runner.runQuery(q3, queryOptions)));
//    printStatus(Quickstart.Color.GREEN, "***************************************************");
//
//    printStatus(Quickstart.Color.GREEN, "***************************************************");
//    printStatus(Quickstart.Color.YELLOW, "Example query run completed.");
//    printStatus(Quickstart.Color.GREEN, "***************************************************");
  }

  @Override
  public String[] getDefaultBatchTableDirectories() {
    return new String[]{
        "examples/batch/splitio_events",
        "examples/batch/splitio_treatments"
    };
  }

  @Override
  public Map<String, Object> getConfigOverrides() {
    Map<String, Object> overrides = new HashMap<>(super.getConfigOverrides());
    overrides.put("pinot.multistage.engine.enabled", "true");
    overrides.put("pinot.server.instance.currentDataTableVersion", 4);
    return overrides;
  }

  public static void main(String[] args)
      throws Exception {
    List<String> arguments = new ArrayList<>();
    arguments.addAll(Arrays.asList("QuickStart", "-type", "MULTI_STAGE"));
    arguments.addAll(Arrays.asList(args));
    PinotAdministrator.main(arguments.toArray(new String[arguments.size()]));
  }
}
