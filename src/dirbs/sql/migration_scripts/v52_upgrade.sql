--
-- DIRBS SQL migration script (v51 -> v52)
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
-- Fix bug in is_unclean_imei
--
DROP FUNCTION is_unclean_imei(TEXT, TEXT);
CREATE FUNCTION is_unclean_imei(imei_norm TEXT, imei TEXT DEFAULT NULL)
    RETURNS BOOLEAN
    LANGUAGE sql IMMUTABLE PARALLEL SAFE
AS $$
    --
    -- An imei can be supplied to this function as an optimization. If we have an IMEI, we can avoid a regex
    -- check for instances where the IMEI norm is not just an uppercase version of the IMEI. If the IMEI norm
    -- is not an uppercase version of the IMEI itself, we know know it is not unclean
    --
    SELECT imei_norm IS NOT NULL
       AND ((imei IS NOT NULL AND UPPER(imei) = imei_norm AND imei_norm !~ '^\d{14}') OR
            (imei IS NULL AND imei_norm !~ '^\d{14}'));
$$;

--
-- Create a view which augments the seen_triplets table with some extra columns showing if fields were NULL
-- or unclean in the data. This can be used to easily find invalid data that has been imported into the system
--
CREATE VIEW seen_triplets_with_invalid_data_flags AS
SELECT *,
       imei_norm IS NULL AS is_null_imei,
       is_unclean_imei(imei_norm) AS is_unclean_imei,
       imsi IS NULL AS is_null_imsi,
       is_unclean_imsi(imsi) AS is_unclean_imsi,
       msisdn IS NULL AS is_null_msisdn
  FROM seen_triplets;
GRANT ALL ON seen_triplets_with_invalid_data_flags TO dirbs_core_import_operator;

--
-- Create a function which takes an import ID and returns the staging table, augmented with columns
-- showing which rows failed NULL, unclean checks
--
CREATE FUNCTION operator_staging_data_with_invalid_data_flags(import_id BIGINT)
    RETURNS TABLE (
        connection_date DATE,
        imei TEXT,
        imei_norm TEXT,
        imsi TEXT,
        msisdn TEXT,
        rat TEXT,
        is_null_imei BOOLEAN,
        is_unclean_imei BOOLEAN,
        is_null_imsi BOOLEAN,
        is_unclean_imsi BOOLEAN,
        is_null_msisdn BOOLEAN,
        is_null_rat BOOLEAN
    )
    LANGUAGE 'plpgsql' IMMUTABLE PARALLEL SAFE
AS $$
BEGIN
    RETURN QUERY EXECUTE FORMAT('SELECT connection_date,
                                        imei,
                                        imei_norm,
                                        imsi,
                                        msisdn,
                                        rat,
                                        imei_norm IS NULL AS is_null_imei,
                                        is_unclean_imei(imei_norm) AS is_unclean_imei,
                                        imsi IS NULL AS is_null_imsi,
                                        is_unclean_imsi(imsi) AS is_unclean_imsi,
                                        msisdn IS NULL AS is_null_msisdn,
                                        rat IS NULL AS is_null_rat
                                   FROM %I',
                                  'staging_operator_import_' || import_id);
END
$$;

--
-- Update operator_data view, fix up some missing permissions and drop an unused sequence
--
DROP VIEW operator_data;
CREATE VIEW operator_data AS
SELECT sq.connection_date,
       sq.imei_norm,
       sq.imsi,
       sq.msisdn
 FROM (SELECT seen_triplets.operator_id,
              ((seen_triplets.import_bitmasks[1] >> 32))::integer AS import_id,
              make_date((seen_triplets.triplet_year)::integer, (seen_triplets.triplet_month)::integer, dom.dom) AS connection_date,
              seen_triplets.imei_norm,
              seen_triplets.imsi,
              seen_triplets.msisdn,
              seen_triplets.triplet_year,
              seen_triplets.triplet_month
         FROM generate_series(1, 31) dom(dom),
              seen_triplets
        WHERE ((seen_triplets.date_bitmask & (1 << (dom.dom - 1))) <> 0)) sq;

GRANT ALL ON operator_data TO dirbs_core_import_operator;
DROP SEQUENCE classification_job_metadata_run_id_seq;
GRANT ALL ON pairing_list_row_id_seq TO dirbs_core_import_pairing_list;
