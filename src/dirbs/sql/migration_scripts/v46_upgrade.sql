--
-- DIRBS SQL migration script (v45 -> v46)
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

DROP FUNCTION triplet_fails_imei_null_check(TEXT);
DROP FUNCTION triplet_fails_imsi_null_check(TEXT);
DROP FUNCTION triplet_fails_msisdn_null_check(TEXT);
DROP FUNCTION triplet_fails_rat_null_check(TEXT);
DROP FUNCTION triplet_fails_clean_check(TEXT, TEXT, TEXT, TEXT, TEXT);
DROP FUNCTION is_valid_subscriber(TEXT);
DROP FUNCTION fails_prefix_check(TEXT, TEXT[]);

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
       AND ((imei IS NOT NULL AND UPPER(imei) = imei_norm AND imei !~ '^\d{14}') OR
            (imei IS NULL AND imei !~ '^\d{14}'));
$$;

CREATE FUNCTION is_unclean_imsi(imsi TEXT)
    RETURNS BOOLEAN
    LANGUAGE sql IMMUTABLE PARALLEL SAFE
AS $$
    --
    -- A NULL IMSI value is not considered unclean. During validation, NULL checks are conducted
    -- separately and we don't treat NULL as unclean so that we can have indepednent checks
    --
    SELECT imsi IS NOT NULL AND NOT LENGTH(imsi) BETWEEN 14 AND 15;
$$;

CREATE FUNCTION is_valid_imsi(imsi TEXT)
    RETURNS BOOLEAN
    LANGUAGE sql IMMUTABLE PARALLEL SAFE
AS $$
    --
    -- A valid IMSI is one that is both non-NULL and clean
    --
    SELECT imsi IS NOT NULL AND NOT is_unclean_imsi(imsi)
$$;

CREATE FUNCTION fails_prefix_check(val text, valid_prefixes_list text[]) RETURNS boolean
    LANGUAGE sql IMMUTABLE PARALLEL SAFE
AS $$
    --
    -- A NULL value is considered to not fail the prefix check
    --
    SELECT val IS NOT NULL AND NOT starts_with_prefix(val, valid_prefixes_list);
$$;
