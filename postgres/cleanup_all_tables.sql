-- =====================================================================
-- Cleanup Script - Delete all data from commercialloan schema
-- Executes deletions in correct order to respect foreign key constraints
-- =====================================================================

-- Set schema
SET search_path TO commercialloan;

-- =====================================================================
-- Option 1: TRUNCATE CASCADE (Fast - removes all data and resets sequences)
-- =====================================================================
-- Uncomment the following lines to use TRUNCATE CASCADE:
/*
TRUNCATE TABLE commercialloan.tblr_agreement_relation CASCADE;
TRUNCATE TABLE commercialloan.tbl_payment_schedule CASCADE;
TRUNCATE TABLE commercialloan.tbl_interest_rate CASCADE;
TRUNCATE TABLE commercialloan.tbl_balance CASCADE;
TRUNCATE TABLE commercialloan.tbl_party CASCADE;
TRUNCATE TABLE commercialloan.tbl_credit_facility CASCADE;
TRUNCATE TABLE commercialloan.tbl_cf_party CASCADE;
TRUNCATE TABLE commercialloan.tbl_agreement CASCADE;
TRUNCATE TABLE commercialloan.tbl_deal CASCADE;
*/

-- =====================================================================
-- Option 2: DELETE (Slower but respects triggers and audit)
-- =====================================================================
-- Delete in correct order respecting foreign keys

-- Credit Facility related tables (children first)
DELETE FROM commercialloan.tblr_agreement_relation;
DELETE FROM commercialloan.tbl_credit_facility;
DELETE FROM commercialloan.tbl_cf_party;

-- Agreement related tables (children first)
DELETE FROM commercialloan.tbl_payment_schedule;
DELETE FROM commercialloan.tbl_interest_rate;
DELETE FROM commercialloan.tbl_balance;
DELETE FROM commercialloan.tbl_party;

-- Parent tables
DELETE FROM commercialloan.tbl_agreement;
DELETE FROM commercialloan.tbl_deal;

-- =====================================================================
-- Option 3: DROP and RECREATE schema (Nuclear option)
-- =====================================================================
-- Uncomment to drop entire schema and recreate:
/*
DROP SCHEMA IF EXISTS commercialloan CASCADE;
CREATE SCHEMA commercialloan;
-- Note: After this, you need to re-run all migrations
*/

-- Verification queries (uncomment to check)
/*
SELECT 'tbl_agreement' AS table_name, COUNT(*) AS row_count FROM commercialloan.tbl_agreement
UNION ALL
SELECT 'tbl_balance', COUNT(*) FROM commercialloan.tbl_balance
UNION ALL
SELECT 'tbl_interest_rate', COUNT(*) FROM commercialloan.tbl_interest_rate
UNION ALL
SELECT 'tbl_party', COUNT(*) FROM commercialloan.tbl_party
UNION ALL
SELECT 'tbl_payment_schedule', COUNT(*) FROM commercialloan.tbl_payment_schedule
UNION ALL
SELECT 'tbl_deal', COUNT(*) FROM commercialloan.tbl_deal
UNION ALL
SELECT 'tbl_credit_facility', COUNT(*) FROM commercialloan.tbl_credit_facility
UNION ALL
SELECT 'tbl_cf_party', COUNT(*) FROM commercialloan.tbl_cf_party
UNION ALL
SELECT 'tblr_agreement_relation', COUNT(*) FROM commercialloan.tblr_agreement_relation;
*/
