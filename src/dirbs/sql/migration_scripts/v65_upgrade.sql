--
-- DIRBS SQL migration script (v64 -> v65)
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
-- Removing imei columns, indexes and copying datas
-- from previous tables (pairing_list, stolen_list, golden_list, registration_list)
--

-- Name: pairing_list; Type: TABLE; Schema: core; Owner: -
CREATE TABLE pairing_list_new (
                               imei_norm text NOT NULL,
                               imsi text NOT NULL,
                               PRIMARY KEY(imei_norm, imsi)
                               );

-- Grant permissions
ALTER TABLE pairing_list_new OWNER TO dirbs_core_import_pairing_list;
GRANT SELECT ON pairing_list_new TO dirbs_core_listgen;
GRANT SELECT ON pairing_list_new TO dirbs_core_report;

-- populate table
INSERT INTO pairing_list_new(imei_norm, imsi)
                             SELECT DISTINCT imei_norm, imsi
                               FROM pairing_list;


-- Name: stolen_list; Type: TABLE; Schema: core; Owner: -
CREATE TABLE stolen_list_new (
                              imei_norm text PRIMARY KEY,
                              reporting_date DATE DEFAULT NULL
                              );

-- Grant permissions
ALTER TABLE stolen_list_new OWNER TO dirbs_core_import_stolen_list;
GRANT SELECT ON stolen_list_new TO dirbs_core_classify;

-- populate table
INSERT INTO stolen_list_new(imei_norm, reporting_date)
                            SELECT imei_norm, MIN(reporting_date) AS reporting_date
                              FROM stolen_list
                          GROUP BY imei_norm;


-- Name: golden_list; Type: TABLE; Schema: core; Owner: -
CREATE TABLE golden_list_new (
                              hashed_imei_norm UUID PRIMARY KEY
                             );

-- Grant permissions
ALTER TABLE golden_list_new OWNER TO dirbs_core_import_golden_list;
GRANT SELECT ON golden_list_new TO dirbs_core_listgen;

-- populate table
INSERT INTO golden_list_new(hashed_imei_norm)
                            SELECT DISTINCT(hashed_imei_norm)
                              FROM golden_list;


-- Name: registration_list; Type: TABLE; Schema: core; Owner: -
CREATE TABLE registration_list_new (
                                    imei_norm text PRIMARY KEY
                                    );

-- Grant permissions
ALTER TABLE registration_list_new OWNER TO dirbs_core_import_registration_list;
GRANT SELECT ON registration_list_new TO dirbs_core_classify;
GRANT SELECT ON registration_list_new TO dirbs_core_api;

-- populate table
INSERT INTO registration_list_new(imei_norm)
                                  SELECT DISTINCT(imei_norm)
                                  FROM registration_list;


-- Drop old tables and rename new ones

-- pairing_list
DROP TABLE pairing_list;
ALTER INDEX pairing_list_new_pkey RENAME to pairing_list_pkey;
ALTER TABLE pairing_list_new RENAME TO pairing_list;

-- stolen_list
DROP TABLE stolen_list;
ALTER INDEX stolen_list_new_pkey RENAME to stolen_list_pkey;
ALTER TABLE stolen_list_new RENAME TO stolen_list;

-- golden_list
DROP TABLE golden_list;
ALTER INDEX golden_list_new_pkey RENAME to golden_list_pkey;
ALTER TABLE golden_list_new RENAME TO golden_list;

-- registration_list
DROP TABLE registration_list;
ALTER INDEX registration_list_new_pkey RENAME to registration_list_pkey;
ALTER TABLE registration_list_new RENAME TO registration_list;



-- Create triggers to Clean/normalize data before inserting

CREATE FUNCTION pairing_list_staging_data_insert_trigger_fn() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    -- Clean/normalize data before inserting
    NEW.imei_norm = normalize_imei(NULLIF(TRIM(NEW.imei), ''));
    RETURN NEW;
END
$$;


CREATE FUNCTION stolen_list_staging_data_insert_trigger_fn() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    -- Clean/normalize data before inserting
    NEW.imei_norm = normalize_imei(NULLIF(TRIM(NEW.imei), ''));
    RETURN NEW;
END
$$;



CREATE FUNCTION registration_list_staging_data_insert_trigger_fn() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    -- Clean/normalize data before inserting
    NEW.imei_norm = normalize_imei(NULLIF(TRIM(NEW.imei), ''));
    RETURN NEW;
END
$$;


CREATE FUNCTION golden_list_prehashed_imei_staging_data_insert_trigger_fn() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    -- Clean/normalize data before inserting
    NEW.hashed_imei_norm = NULLIF(TRIM(NEW.golden_imei), '')::UUID;
    RETURN NEW;
END
$$;


CREATE FUNCTION golden_list_unhashed_imei_staging_data_insert_trigger_fn() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    -- Clean/normalize data before inserting
    NEW.hashed_imei_norm =  MD5(normalize_imei(NULLIF(TRIM(NEW.golden_imei), '')))::UUID;
    RETURN NEW;
END
$$;
