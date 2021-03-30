--
-- DIRBS SQL migration script (v29 -> v30)
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

ALTER TABLE seen_imeis ADD COLUMN seen_rat_bitmask INTEGER;

CREATE FUNCTION triplet_fails_rat_null_check(rat text) RETURNS boolean
    LANGUAGE sql IMMUTABLE PARALLEL SAFE
    AS $$
    SELECT rat is NULL;
$$;

DROP FUNCTION triplet_fails_clean_check(imei text, imei_norm text, imsi text, msisdn text);

CREATE FUNCTION triplet_fails_clean_check(imei text, imei_norm text, imsi text, msisdn text, rat text default '000')
    RETURNS boolean
    LANGUAGE sql IMMUTABLE PARALLEL SAFE
    AS $$
    SELECT msisdn IS NULL
           OR NOT COALESCE(LENGTH(imsi) BETWEEN 14 AND 15, FALSE)
           OR (UPPER(imei) = imei_norm AND imei !~ '^\d{14}')
           OR rat is NULL;
$$;

CREATE TABLE radio_access_technology_map (
    rat_code TEXT NOT NULL,
    technology_name TEXT NOT NULL,
    operator_rank SMALLINT NOT NULL,
    gsma_rank SMALLINT NOT NULL,
    technology_generation TEXT NOT NULL
)
WITH (fillfactor='100');

INSERT INTO radio_access_technology_map(rat_code, technology_name, operator_rank, gsma_rank,
                                        technology_generation) VALUES
    ('007', 'Virtual', 1, 3, 'Non-cellular'),
    ('004', 'GAN', 2, 3, 'Non-cellular'),
    ('003',  'WLAN', 3, 3, 'Non-cellular'),
    ('104', '3GPP2 1xRTT', 4, 6,  '2G'),
    ('002', 'GERAN', 5, 6, '2G'),
    ('103', '3GPP2 HRPD', 6, 9, '3G'),
    ('001', 'UTRAN', 7, 9, '3G'),
    ('102', '3GPP2 eHRPD', 8, 9, '3G'),
    ('005', 'HSPA Evolution', 9, 9, '3G'),
    ('105', '3GPP2 UMB', 10, 12, '4G'),
    ('101', 'IEEE 802.16', 11, 12, '4G'),
    ('006', 'E-UTRAN', 12, 12, '4G');

CREATE FUNCTION translate_rat_code_to_bitmask(rat text) RETURNS INTEGER
    LANGUAGE sql IMMUTABLE PARALLEL SAFE
    AS $$
    SELECT
        bit_or(1 << operator_rank) AS bitmask
    FROM(
    SELECT operator_rank
      FROM (SELECT (regexp_matches(rat, E'([0-9]{3})','g'))[1] AS rat_code) rat_list
    JOIN radio_access_technology_map rat_map
    ON rat_list.rat_code = rat_map.rat_code) rat_rank
$$;

CREATE FUNCTION translate_bands_to_rat_bitmask(bands text) RETURNS INTEGER
  LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE
    AS $$
    DECLARE
      bitmask INTEGER := 0;
    BEGIN
        IF bands LIKE '%LTE%' OR bands LIKE '%CA_%' OR bands LIKE '%DC_%'
                     OR bands LIKE '%WiMAX%' OR bands LIKE '%UMB%'
        THEN bitmask = bitmask | (1 << 12);
        END IF;

        IF bands LIKE '%HSPA%' OR bands LIKE '%HSUPA%' OR bands LIKE '%HSDPA%'
                     OR bands LIKE '%EVDO%' OR bands LIKE '%WCDMA%' OR bands LIKE '%UMTS%'
                     OR bands LIKE '%TDS-CDMA%' OR bands LIKE '%TD-SCDMA%'
                     OR bands LIKE '%CDMA2000%'
        THEN bitmask = bitmask | (1 << 9);
        END IF;

        IF bands LIKE '%GSM%' OR bands LIKE '%GPRS%' OR bands LIKE '%EDGE%' OR bands LIKE '% CDMA %'
        THEN bitmask = bitmask | (1 << 6);
        END IF;

        IF bitmask = 0
        THEN bitmask = bitmask | (1 << 3);
        END IF;
        RETURN bitmask;
    END
$$;

ALTER TABLE gsma_data ADD COLUMN rat_bitmask INTEGER;

UPDATE gsma_data
   SET rat_bitmask = translate_bands_to_rat_bitmask(bands);
