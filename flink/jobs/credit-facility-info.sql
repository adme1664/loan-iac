-- Composes one document per credit facility: the facility and its deal (from
-- the deal topic) with the agreements listed in its agreementRelations (from
-- the three agreement topics), and publishes it to 3857.dev.credit.facility.info.

SET 'pipeline.name' = 'credit-facility-info';

-- Changes are buffered for a few seconds and applied together, so a facility
-- is published once with all the agreements that arrived in that time, not
-- once per agreement
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '5 s';
SET 'table.exec.mini-batch.size' = '5000';

-- The connectors write values in the Schema Registry JSON format: 5 header
-- bytes followed by the JSON text. Flink has no format for it, so values are
-- read as raw text and the header is cut off in the views below.

-- Latest deal per deal number
CREATE TABLE deal_source (
  deal_no STRING,
  raw_value STRING,
  PRIMARY KEY (deal_no) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = '3857.dev.deal.v1-deal_view',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'flink-credit-facility-info',
  'key.format' = 'raw',
  'value.format' = 'raw',
  'value.fields-include' = 'EXCEPT_KEY'
);

-- Latest agreement per agreement number: loans, lines of credit and letters
CREATE TABLE agreement_source (
  agreement_no STRING,
  raw_value STRING,
  PRIMARY KEY (agreement_no) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = '3857.dev.loan.agreements.v1-loan_view;3857.dev.loc.agreements.v1-loc_view;3857.dev.letter.agreements.v1-letter_view',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'flink-credit-facility-info',
  'key.format' = 'raw',
  'value.format' = 'raw',
  'value.fields-include' = 'EXCEPT_KEY'
);

-- One composite per credit facility number
CREATE TABLE credit_facility_sink (
  credit_facility_no STRING,
  composite STRING,
  PRIMARY KEY (credit_facility_no) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = '3857.dev.credit.facility.info',
  'properties.bootstrap.servers' = 'kafka:9092',
  'key.format' = 'raw',
  'value.format' = 'raw',
  'value.fields-include' = 'EXCEPT_KEY'
);

CREATE TEMPORARY VIEW deals AS
SELECT deal_no, SUBSTRING(raw_value FROM POSITION('{' IN raw_value)) AS deal_json
FROM deal_source;

CREATE TEMPORARY VIEW agreements AS
SELECT agreement_no, SUBSTRING(raw_value FROM POSITION('{' IN raw_value)) AS agreement_json
FROM agreement_source;

-- One row per credit facility of each deal
CREATE TEMPORARY VIEW credit_facilities AS
SELECT
  JSON_VALUE(facility_json, '$.creditFacilityNo') AS credit_facility_no,
  JSON_QUERY(d.deal_json, '$.deal') AS deal_json,
  facility_json
FROM deals AS d
CROSS JOIN UNNEST(JSON_QUERY(d.deal_json, '$.creditFacilities' RETURNING ARRAY<STRING>)) AS f (facility_json);

-- One row per agreement relation of each credit facility, carrying the
-- facility and its deal
CREATE TEMPORARY VIEW agreement_relations AS
SELECT
  cf.credit_facility_no,
  cf.deal_json,
  cf.facility_json,
  JSON_VALUE(relation_json, '$.agreementId') AS agreement_no
FROM credit_facilities AS cf
CROSS JOIN UNNEST(JSON_QUERY(cf.facility_json, '$.agreementRelations' RETURNING ARRAY<STRING>)) AS r (relation_json);

-- The join: a relation's agreementId is the agreement number. The result is
-- grouped per credit facility, with its agreements gathered into one array.
-- The aggregation is deliberately the last step: it emits a plain update per
-- facility, where a join placed after it would emit a delete (a tombstone in
-- the topic) followed by an insert on every change.
INSERT INTO credit_facility_sink
SELECT
  r.credit_facility_no,
  '{"creditFacilityNo":' || JSON_STRING(r.credit_facility_no)
    || ',"deal":' || LAST_VALUE(r.deal_json)
    || ',"creditFacility":' || LAST_VALUE(r.facility_json)
    || ',"agreements":[' || LISTAGG(a.agreement_json, ',') || ']}'
FROM agreement_relations AS r
JOIN agreements AS a ON a.agreement_no = r.agreement_no
GROUP BY r.credit_facility_no;
