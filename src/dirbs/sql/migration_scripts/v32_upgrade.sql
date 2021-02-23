--
-- DIRBS SQL migration script (v31 -> v32)
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
-- Rename current report_metadata table to report_data_metadata, as it stores metadata
-- about a report data version, rather than about the reporting run itself
--
ALTER TABLE report_metadata RENAME TO report_data_metadata;
ALTER TABLE report_data_metadata DROP COLUMN metadata;
ALTER TABLE report_data_metadata RENAME COLUMN run_id TO data_id;
ALTER SEQUENCE report_metadata_run_id_seq RENAME TO report_data_metadata_data_id_seq;
ALTER INDEX report_metadata_pkey RENAME TO report_data_metadata_pkey;

--
-- Create new table for storing dirbs-report metadata
--
CREATE TABLE report_job_metadata (
    run_id SERIAL PRIMARY KEY,
    run_date TIMESTAMPTZ NOT NULL,
    metadata JSONB NOT NULL
);

--
-- Create new table for storing dirbs-purge metadata
--
CREATE TABLE purge_job_metadata (
    run_id SERIAL PRIMARY KEY,
    run_date TIMESTAMPTZ NOT NULL,
    metadata JSONB NOT NULL
);

--
-- Create new table for storing dirbs-listgen metadata
--
CREATE TABLE listgen_job_metadata (
    run_id SERIAL PRIMARY KEY,
    run_date TIMESTAMPTZ NOT NULL,
    metadata JSONB NOT NULL
);

--
-- Create new table for storing dirbs-db metadata
--
CREATE TABLE db_job_metadata (
    run_id SERIAL PRIMARY KEY,
    run_date TIMESTAMPTZ NOT NULL,
    metadata JSONB NOT NULL
);

--
-- Rename classification_metadata to match new table pattern
--
ALTER TABLE classification_metadata RENAME TO classification_job_metadata;
ALTER SEQUENCE classification_run_id_seq RENAME TO classification_job_metadata_run_id_seq;
ALTER INDEX classification_metadata_pkey RENAME TO classification_job_metadata_pkey;
ALTER TABLE classification_job_metadata ALTER COLUMN run_id
    SET DEFAULT NEXTVAL('classification_job_metadata_run_id_seq'::regclass);

--
-- Rename import_metadata to match new table pattern
--
ALTER TABLE import_metadata RENAME TO import_job_metadata;
ALTER TABLE import_job_metadata RENAME COLUMN import_id TO run_id;
ALTER TABLE import_job_metadata RENAME COLUMN import_date TO run_date;
ALTER SEQUENCE import_id_seq RENAME TO import_job_metadata_run_id_seq;
ALTER SEQUENCE import_job_metadata_run_id_seq OWNED BY import_job_metadata.run_id;
ALTER TABLE import_job_metadata ALTER COLUMN run_id
    SET DEFAULT NEXTVAL('import_job_metadata_run_id_seq'::regclass);
ALTER INDEX import_metadata_pkey RENAME TO import_job_metadata_pkey;

--
-- Rename run_id columns on tables
--
ALTER TABLE report_blacklist_violation_stats RENAME COLUMN run_id TO data_id;
ALTER TABLE report_blacklist_violation_stats RENAME CONSTRAINT report_blacklist_violation_stats_run_id_fkey
    TO report_blacklist_violation_stats_data_id_fkey;

ALTER TABLE report_daily_stats RENAME COLUMN run_id TO data_id;
ALTER TABLE report_daily_stats RENAME CONSTRAINT report_daily_stats_run_id_fkey TO report_daily_stats_data_id_fkey;

ALTER TABLE report_monthly_condition_stats RENAME COLUMN run_id TO data_id;
ALTER TABLE report_monthly_condition_stats RENAME CONSTRAINT report_monthly_condition_stats_run_id_fkey
    TO report_monthly_condition_stats_data_id_fkey;

ALTER TABLE report_monthly_stats RENAME COLUMN run_id TO data_id;
ALTER TABLE report_monthly_stats RENAME CONSTRAINT report_monthly_stats_run_id_fkey
    TO report_monthly_stats_data_id_fkey;

ALTER TABLE report_monthly_top_models_gross_adds RENAME COLUMN run_id TO data_id;
ALTER TABLE report_monthly_top_models_gross_adds ADD CONSTRAINT report_monthly_top_models_gross_adds_data_id_fkey
    FOREIGN KEY (data_id) REFERENCES report_data_metadata(data_id);

ALTER TABLE report_monthly_top_models_imei RENAME COLUMN run_id TO data_id;
ALTER TABLE report_monthly_top_models_imei RENAME CONSTRAINT report_monthly_top_models_imei_run_id_fkey
    TO report_monthly_top_models_imei_data_id_fkey;
