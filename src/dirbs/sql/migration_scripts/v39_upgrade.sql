--
-- DIRBS SQL migration script (v38 -> v39)
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
-- Create type for command
--
CREATE TYPE job_command_type AS ENUM (
    'dirbs-catalog',
    'dirbs-classify',
    'dirbs-db',
    'dirbs-import',
    'dirbs-listgen',
    'dirbs-prune',
    'dirbs-report'
);


--
-- Create enum type for job status
--
CREATE TYPE job_status_type AS ENUM (
    'running',
    'success',
    'error',
    'unknown'
);


--
-- Create new table for storing metadata globally
--
CREATE TABLE job_metadata (
    PRIMARY KEY (command, run_id),
    command         job_command_type NOT NULL,
    run_id          BIGINT NOT NULL,
    start_time      TIMESTAMPTZ NOT NULL,
    end_time        TIMESTAMPTZ DEFAULT NULL,
    db_user         TEXT,
    subcommand      TEXT,
    command_line    TEXT,
    status          job_status_type NOT NULL,
    exception_info  TEXT DEFAULT NULL,
    extra_metadata  JSONB NOT NULL DEFAULT '{}'::JSONB
) WITH (fillfactor = 80);

--
-- Create additional indexes
--
CREATE INDEX ON job_metadata(run_id);
CREATE INDEX ON job_metadata(status);
CREATE INDEX ON job_metadata(start_time);

--
-- Migrate existing classification metadata across
--
INSERT INTO job_metadata(command,
                         run_id,
                         start_time,
                         db_user,
                         subcommand,
                         command_line,
                         status,
                         extra_metadata)
    SELECT 'dirbs-classify',
           cm.run_id,
           cm.run_date,
           NULL,
           NULL,
           NULL,
           'unknown',
           cm.metadata
      FROM classification_job_metadata cm;


--
-- Migrate existing db metadata across
--
INSERT INTO job_metadata(command,
                         run_id,
                         start_time,
                         db_user,
                         subcommand,
                         command_line,
                         status,
                         extra_metadata)
    SELECT 'dirbs-db',
           dm.run_id,
           dm.run_date,
           NULL,
           NULL,
           NULL,
           'unknown',
           dm.metadata
      FROM db_job_metadata dm;


--
-- Migrate existing import metadata across
--
INSERT INTO job_metadata(command,
                         run_id,
                         start_time,
                         db_user,
                         subcommand,
                         command_line,
                         status,
                         extra_metadata)
    SELECT 'dirbs-import',
           im.run_id,
           im.run_date,
           NULL,
           im.import_type,
           NULL,
           'unknown',
           im.metadata
      FROM import_job_metadata im;


--
-- Migrate existing listgen metadata across
--
INSERT INTO job_metadata(command,
                         run_id,
                         start_time,
                         db_user,
                         subcommand,
                         command_line,
                         status,
                         extra_metadata)
    SELECT 'dirbs-listgen',
           lm.run_id,
           lm.run_date,
           NULL,
           NULL,
           NULL,
           'unknown',
           lm.metadata
      FROM listgen_job_metadata lm;


--
-- Migrate existing prune metadata across
--
INSERT INTO job_metadata(command,
                         run_id,
                         start_time,
                         db_user,
                         subcommand,
                         command_line,
                         status,
                         extra_metadata)
    SELECT 'dirbs-prune',
           pm.run_id,
           pm.run_date,
           NULL,
           NULL,
           NULL,
           'unknown',
           pm.metadata
      FROM prune_job_metadata pm;


--
-- Migrate existing report metadata across
--
INSERT INTO job_metadata(command,
                         run_id,
                         start_time,
                         db_user,
                         subcommand,
                         command_line,
                         status,
                         extra_metadata)
    SELECT 'dirbs-report',
           rm.run_id,
           rm.run_date,
           NULL,
           NULL,
           NULL,
           'unknown',
           rm.metadata
      FROM report_job_metadata rm;


--
-- Now that all data is in, we can make the run_id autoincrementing by adding a sequence
--
CREATE SEQUENCE job_metadata_run_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
SELECT setval('job_metadata_run_id_seq', (SELECT MAX(run_id) FROM job_metadata));
ALTER SEQUENCE job_metadata_run_id_seq OWNED BY job_metadata.run_id;
ALTER TABLE ONLY job_metadata ALTER COLUMN run_id SET DEFAULT nextval('job_metadata_run_id_seq'::regclass);


--
-- Now we can drop all the old metadata tables
--
DROP TABLE classification_job_metadata;
DROP TABLE db_job_metadata;
DROP TABLE import_job_metadata;
DROP TABLE listgen_job_metadata;
DROP TABLE prune_job_metadata;
DROP TABLE report_job_metadata;
