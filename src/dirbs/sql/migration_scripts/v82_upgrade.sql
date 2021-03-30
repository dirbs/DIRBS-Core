--
-- DIRBS SQL migration script (v81 -> v82)
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
-- Add model_number, brand_name, device_type, radio_interface columns to registration list importer
--
ALTER TABLE historic_registration_list ADD COLUMN model_number TEXT;
ALTER TABLE historic_registration_list ADD COLUMN brand_name TEXT;
ALTER TABLE historic_registration_list ADD COLUMN device_type TEXT;
ALTER TABLE historic_registration_list ADD COLUMN radio_interface TEXT;
CREATE OR REPLACE VIEW registration_list AS
    SELECT imei_norm, make, model, status, virt_imei_shard, model_number, brand_name, device_type, radio_interface,
    substring(imei_norm FROM 1 FOR 8) AS tac
      FROM historic_registration_list
     WHERE end_date IS NULL WITH CHECK OPTION;

DROP FUNCTION registration_list_staging_data_insert_trigger_fn() CASCADE;

-- convert optional fields white-space only to null
CREATE FUNCTION registration_list_staging_data_insert_trigger_fn() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    -- Clean/normalize data before inserting
    NEW.imei_norm = normalize_imei(NULLIF(TRIM(NEW.imei), ''));
    NEW.make = NULLIF(TRIM(NEW.make), '');
    NEW.model = NULLIF(TRIM(NEW.model), '');
    -- status value will be used to filter for status 'register' in classification and needs to be lowercase
    NEW.status = LOWER(NULLIF(TRIM(NEW.status), ''));
    NEW.model_number = NULLIF(TRIM(NEW.model_number), '');
    NEW.brand_name = NULLIF(TRIM(NEW.brand_name), '');
    NEW.device_type = NULLIF(TRIM(NEW.device_type), '');
    NEW.radio_interface = NULLIF(TRIM(NEW.radio_interface), '');
    RETURN NEW;
END
$$;

-- grant dirbs-api-user to read data from notifications_lists & blacklist
GRANT SELECT ON notifications_lists TO dirbs_core_api;
GRANT SELECT ON blacklist TO dirbs_core_api;
