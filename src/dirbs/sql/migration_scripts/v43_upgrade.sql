--
-- DIRBS SQL migration script (v42 -> v43)
--
-- Copyright (c) 2018-2021 Qualcomm Technologies, Inc.
--
-- All rights reserved.
--
-- Redistribution and use in source and binary forms, with or without modification, are permitted (subject to the
-- limitations in the disclaimer below) provided that the following conditions are met:
--
-- - Redistributions of source code must retain the above copyright notice, this list of conditions and the following
--   disclaimer.
-- - Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following
--   disclaimer in the documentation and/or other materials provided with the distribution.
-- - Neither the name of Qualcomm Technologies, Inc. nor the names of its contributors may be used to endorse or promote
--   products derived from this software without specific prior written permission.
-- - The origin of this software must not be misrepresented; you must not claim that you wrote the original software.
--   If you use this software in a product, an acknowledgment is required by displaying the trademark/logo as per the
--   details provided here: https://www.qualcomm.com/documents/dirbs-logo-and-brand-guidelines
-- - Altered source versions must be plainly marked as such, and must not be misrepresented as being the original software.
-- - This notice may not be removed or altered from any source distribution.
--
-- NO EXPRESS OR IMPLIED LICENSES TO ANY PARTY'S PATENT RIGHTS ARE GRANTED BY THIS LICENSE. THIS SOFTWARE IS PROVIDED BY
-- THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
-- THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
-- COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
-- DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
-- BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
-- (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
-- POSSIBILITY OF SUCH DAMAGE.
--

--
-- Add a new integer schema_version to the report_data_metadata column and default to 1
--
ALTER TABLE report_data_metadata ADD COLUMN data_schema_version INTEGER NOT NULL DEFAULT 1;
ALTER TABLE report_data_metadata ALTER COLUMN data_schema_version DROP DEFAULT;

--
-- Add table for conditions used in a data run along with their config
--
CREATE TABLE report_monthly_conditions (
    data_id                 INTEGER NOT NULL,
    cond_name               TEXT NOT NULL,
    sort_order              INTEGER NOT NULL,
    was_blocking            BOOLEAN NOT NULL,
    last_successful_config  JSONB NOT NULL
);

--
-- Add foreign key linking data_id to report_data_metadata
--
ALTER TABLE report_monthly_conditions ADD PRIMARY KEY (data_id, cond_name);
ALTER TABLE report_monthly_conditions ADD FOREIGN KEY (data_id) REFERENCES report_data_metadata(data_id);

--
-- Now that we store conditions in a separate, global list we can removing was_blocking from the per-operator table
-- and add a foreign key to the global table
--
ALTER TABLE report_monthly_condition_stats DROP COLUMN was_blocking;
ALTER TABLE report_monthly_condition_stats ADD
    FOREIGN KEY (data_id, cond_name) REFERENCES report_monthly_conditions(data_id, cond_name);

--
-- Add new columns to report_monthly_stats
--
ALTER TABLE report_monthly_stats ADD COLUMN num_records BIGINT;
ALTER TABLE report_monthly_stats ADD COLUMN num_null_imsis BIGINT;
ALTER TABLE report_monthly_stats ADD COLUMN num_null_msisdns BIGINT;
ALTER TABLE report_monthly_stats ADD COLUMN num_whitespace_imsis BIGINT;
ALTER TABLE report_monthly_stats ADD COLUMN num_whitespace_msisdns BIGINT;
ALTER TABLE report_monthly_stats ADD COLUMN num_invalid_imei_imsis BIGINT;
ALTER TABLE report_monthly_stats ADD COLUMN num_invalid_imei_msisdns BIGINT;
ALTER TABLE report_monthly_stats ADD COLUMN num_invalid_triplets BIGINT;
ALTER TABLE report_monthly_stats ADD COLUMN num_imei_imsis BIGINT;
ALTER TABLE report_monthly_stats ADD COLUMN num_imei_msisdns BIGINT;
ALTER TABLE report_monthly_stats ADD COLUMN num_imsi_msisdns BIGINT;
ALTER TABLE report_monthly_stats ADD COLUMN num_compliant_imei_imsis BIGINT;
ALTER TABLE report_monthly_stats ADD COLUMN num_noncompliant_imei_imsis BIGINT;
ALTER TABLE report_monthly_stats ADD COLUMN num_noncompliant_imei_imsis_blocking BIGINT;
ALTER TABLE report_monthly_stats ADD COLUMN num_noncompliant_imei_imsis_info_only BIGINT;
ALTER TABLE report_monthly_stats ADD COLUMN num_compliant_imei_msisdns BIGINT;
ALTER TABLE report_monthly_stats ADD COLUMN num_noncompliant_imei_msisdns BIGINT;
ALTER TABLE report_monthly_stats ADD COLUMN num_noncompliant_imei_msisdns_blocking BIGINT;
ALTER TABLE report_monthly_stats ADD COLUMN num_noncompliant_imei_msisdns_info_only BIGINT;

--
-- Add new columns to report_monthly_condition_stats
--
ALTER TABLE report_monthly_condition_stats ADD COLUMN num_imei_imsis BIGINT;
ALTER TABLE report_monthly_condition_stats ADD COLUMN num_imei_msisdns BIGINT;
ALTER TABLE report_monthly_condition_stats ADD COLUMN num_imei_gross_adds BIGINT;

--
-- Add table for IMEI-IMSI overloading stats
--
CREATE TABLE report_monthly_imei_imsi_overloading (
    data_id                 INTEGER NOT NULL,
    num_imeis               BIGINT NOT NULL,
    seen_with_imsis         BIGINT NOT NULL,
    operator_id             TEXT NOT NULL
);

ALTER TABLE report_monthly_imei_imsi_overloading ADD PRIMARY KEY (data_id, operator_id, seen_with_imsis);
ALTER TABLE report_monthly_imei_imsi_overloading ADD FOREIGN KEY (data_id) REFERENCES report_data_metadata(data_id);

--
-- Add table for IMEI-IMSI overloading stats
--
CREATE TABLE report_monthly_imsi_imei_overloading (
    data_id                 INTEGER NOT NULL,
    num_imsis               BIGINT NOT NULL,
    seen_with_imeis         BIGINT NOT NULL,
    operator_id             TEXT NOT NULL
);

ALTER TABLE report_monthly_imsi_imei_overloading ADD PRIMARY KEY (data_id, operator_id, seen_with_imeis);
ALTER TABLE report_monthly_imsi_imei_overloading ADD FOREIGN KEY (data_id) REFERENCES report_data_metadata(data_id);

--
-- Add table for per-condition combinatorial stats
--
CREATE TABLE report_monthly_condition_stats_combinations (
    data_id                 INTEGER NOT NULL,
    combination             BOOLEAN[] NOT NULL,
    num_imeis               BIGINT NOT NULL,
    num_imei_gross_adds     BIGINT NOT NULL,
    num_imei_imsis          BIGINT NOT NULL,
    num_imei_msisdns        BIGINT NOT NULL,
    num_subscriber_triplets BIGINT NOT NULL,
    compliance_level        SMALLINT NOT NULL,
    operator_id             TEXT NOT NULL
);

ALTER TABLE report_monthly_condition_stats_combinations ADD PRIMARY KEY (data_id, operator_id, combination);
ALTER TABLE report_monthly_condition_stats_combinations ADD FOREIGN KEY (data_id) REFERENCES report_data_metadata(data_id);

--
-- Rename report_blacklist_violation_stats to report_monthly_blacklist_violation_stats
--
ALTER TABLE report_blacklist_violation_stats RENAME TO report_monthly_blacklist_violation_stats;
ALTER INDEX report_blacklist_violation_stats_pkey RENAME TO report_monthly_blacklist_violation_stats_pkey;
ALTER TABLE report_monthly_blacklist_violation_stats
    RENAME CONSTRAINT report_blacklist_violation_stats_data_id_fkey TO report_monthly_blacklist_violation_stats_data_id_fkey;

