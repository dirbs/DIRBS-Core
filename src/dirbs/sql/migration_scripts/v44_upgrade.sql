--
-- DIRBS SQL migration script (v43 -> v44)
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

-- Everyone should be able to read the schema_metadata and schema_version tables
GRANT SELECT ON schema_metadata, schema_version TO dirbs_core_base;

-- Jobs needs to be able to read and write job_metadata tables
GRANT SELECT, INSERT, UPDATE ON job_metadata TO dirbs_core_job;
GRANT USAGE ON job_metadata_run_id_seq TO dirbs_core_job;

-- Grant permissions for listgen role
GRANT SELECT ON classification_state, pairing_list, golden_list, seen_triplets TO dirbs_core_listgen;

-- Grant permissions for operator data importer role
GRANT SELECT ON radio_access_technology_map,
                gsma_data,
                report_daily_stats TO dirbs_core_import_operator;
ALTER TABLE seen_triplets OWNER TO dirbs_core_import_operator;
ALTER TABLE seen_imeis OWNER TO dirbs_core_import_operator;

-- Grant permissions for pairing list importer role
ALTER TABLE pairing_list OWNER TO dirbs_core_import_pairing_list;

-- Grant permissions for stolen list importer role
ALTER TABLE stolen_list OWNER TO dirbs_core_import_stolen_list;

-- Grant permissions for registration list importer role
ALTER TABLE registration_list OWNER TO dirbs_core_import_registration_list;

-- Grant permissions for golden list importer role
ALTER TABLE golden_list OWNER TO dirbs_core_import_golden_list;

-- Grant permissions for GSMA TAC DB importer role
ALTER TABLE gsma_data OWNER TO dirbs_core_import_gsma;

-- Grant permissions for classify role
GRANT SELECT ON seen_triplets,
                seen_imeis,
                gsma_data,
                stolen_list,
                registration_list TO dirbs_core_classify;
GRANT SELECT, INSERT, UPDATE ON classification_state TO dirbs_core_classify;
GRANT SELECT, UPDATE, USAGE ON classification_state_row_id_seq TO dirbs_core_classify;

-- Grant permissions for report role
GRANT SELECT, INSERT, UPDATE ON report_daily_stats,
                                report_data_metadata,
                                report_monthly_blacklist_violation_stats,
                                report_monthly_condition_stats,
                                report_monthly_condition_stats_combinations,
                                report_monthly_conditions,
                                report_monthly_imei_imsi_overloading,
                                report_monthly_imsi_imei_overloading,
                                report_monthly_stats,
                                report_monthly_top_models_gross_adds,
                                report_monthly_top_models_imei TO dirbs_core_report;
GRANT USAGE ON report_data_metadata_data_id_seq TO dirbs_core_report;
GRANT SELECT ON seen_triplets,
                seen_imeis,
                gsma_data,
                pairing_list,
                radio_access_technology_map,
                classification_state TO dirbs_core_report;

-- Grant permissions for catalog role
GRANT SELECT, INSERT, UPDATE ON data_catalog TO dirbs_core_catalog;
GRANT USAGE ON data_catalog_file_id_seq TO dirbs_core_catalog;

-- Grant permissions for API role
GRANT SELECT ON gsma_data,
                classification_state,
                seen_triplets,
                data_catalog,
                schema_metadata,
                schema_version,
                job_metadata TO dirbs_core_api;
