--
-- DIRBS SQL migration script (v82 -> v83)
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
-- Alter historic_registration_list table & view to support DRS device mapping
--
ALTER TABLE historic_registration_list ADD COLUMN device_id TEXT;

--
-- Alter registration_list view to support device mapping
--
DROP VIEW registration_list;
CREATE VIEW registration_list AS
    SELECT imei_norm, make, model, status, virt_imei_shard, model_number,
    brand_name, device_type, radio_interface, device_id
      FROM historic_registration_list
     WHERE end_date IS NULL WITH CHECK OPTION;

--
-- alter registration_list_staging_data_insert_trigger_fn() to support device mapping
--
DROP FUNCTION registration_list_staging_data_insert_trigger_fn() CASCADE;
CREATE FUNCTION registration_list_staging_data_insert_trigger_fn() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    NEW.imei_norm = normalize_imei(NULLIF(TRIM(NEW.imei), ''));
    NEW.make = NULLIF(TRIM(NEW.make), '');
    NEW.model = NULLIF(TRIM(NEW.model), '');
    NEW.status = LOWER(NULLIF(TRIM(NEW.status), ''));
    NEW.model_number = NULLIF(TRIM(NEW.model_number), '');
    NEW.brand_name = NULLIF(TRIM(NEW.brand_name), '');
    NEW.device_type = NULLIF(TRIM(NEW.device_type), '');
    NEW.radio_interface = NULLIF(TRIM(NEW.radio_interface), '');
    RETURN NEW;
END
$$;

--
-- grant necessary permissions
--
GRANT SELECT ON registration_list TO dirbs_core_api, dirbs_core_classify, dirbs_core_import_registration_list;

--
-- function to check un-clean MSISDN
--
CREATE FUNCTION is_unclean_msisdn(msisdn TEXT)
    RETURNS BOOLEAN
    LANGUAGE sql IMMUTABLE PARALLEL SAFE
AS $$
  --
  -- A NULL MSISDN value is not considered unclean. During validation, NULL checks are conducted
  -- separately and we don't treat NULL as unclean so that we can have independent checks
  --
  SELECT msisdn IS NOT NULL AND NOT LENGTH(msisdn) BETWEEN 12 AND 15;
$$;

--
-- function to check validity of MSISDN
--
CREATE FUNCTION is_valid_msisdn(msisdn TEXT)
    RETURNS BOOLEAN
    LANGUAGE sql IMMUTABLE PARALLEL SAFE
AS $$
  --
  -- A valid MSISDN is one that is both non-NULL and clean
  --
  SELECT msisdn IS NOT NULL and NOT is_unclean_msisdn(msisdn)
$$;