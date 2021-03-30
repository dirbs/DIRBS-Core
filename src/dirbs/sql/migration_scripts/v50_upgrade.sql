--
-- DIRBS SQL migration script (v49 -> v50)
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
-- Remove NOT NULL constraint from seen_triplets table and all child tables
--
ALTER TABLE seen_triplets ALTER COLUMN imei_norm DROP NOT NULL;

--
-- Now that IMEI norm can be NULL, we need to make sure we COALESCE inside hash_triplet
--
DROP FUNCTION hash_triplet(text, text, text);
CREATE FUNCTION hash_triplet(imei_norm text, imsi text, msisdn text) RETURNS uuid
    LANGUAGE sql IMMUTABLE PARALLEL SAFE
    AS $$
    SELECT MD5(COALESCE(imei_norm, '') || '@' ||
               COALESCE(imsi, '') || '@' ||
               COALESCE(msisdn, ''))::UUID;
$$;

--
-- Store report stats on the number of records with a NULL IMEI
--
ALTER TABLE report_monthly_stats ADD COLUMN num_null_imei_records BIGINT;
ALTER TABLE report_monthly_stats RENAME COLUMN num_null_imsis TO num_null_imsi_records;
ALTER TABLE report_monthly_stats RENAME COLUMN num_null_msisdns TO num_null_msisdn_records;
ALTER TABLE report_monthly_stats RENAME COLUMN num_whitespace_imsis TO num_whitespace_imsi_records;
ALTER TABLE report_monthly_stats RENAME COLUMN num_whitespace_msisdns TO num_whitespace_msisdn_records;

--
-- Create a view that filters out the non-records with NULL IMEIs from seen_triplets
--
CREATE VIEW seen_triplets_no_null_imeis AS SELECT * FROM seen_triplets WHERE imei_norm IS NOT NULL;
GRANT SELECT ON seen_triplets_no_null_imeis TO dirbs_core_listgen;
GRANT SELECT ON seen_triplets_no_null_imeis TO dirbs_core_classify;
GRANT SELECT ON seen_triplets_no_null_imeis TO dirbs_core_report;
GRANT SELECT ON seen_triplets_no_null_imeis TO dirbs_core_api;
