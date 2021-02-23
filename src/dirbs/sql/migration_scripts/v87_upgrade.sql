--
-- DIRBS SQL migration script (v86 -> v87)
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

-- Create a function to find weather msisdns asociated with an imei forms any kind of arithmetic progression
CREATE FUNCTION have_arithmetic_progression(imei TEXT, analysis_start_date DATE, analysis_end_date DATE)
    RETURNS BOOLEAN
    LANGUAGE sql IMMUTABLE PARALLEL SAFE
    -- Detects arithmetic progression in list of MSISDNs obtained from
    -- IMEI and analysis dates
AS $$
    SELECT (MIN(msisdn - prev_msisdn) = MAX(msisdn - prev_msisdn)) AS is_arithmetic_progression
      FROM (SELECT imt.*, LAG(msisdn) OVER (ORDER BY imei_norm) AS prev_msisdn
          FROM(SELECT DISTINCT imei_norm::bigint, msisdn::bigint
                 FROM monthly_network_triplets_country
                WHERE imei_norm = imei
                  AND last_seen >= analysis_start_date::date
                  AND first_seen < analysis_end_date::date) imt ) t
$$;


-- Create a function to check if msisdns belong to the same operator given imei and dates
CREATE FUNCTION have_same_operator_id(imei TEXT, analysis_start_date DATE, analysis_end_date DATE)
    RETURNS BIGINT
    LANGUAGE sql IMMUTABLE PARALLEL SAFE
AS $$
    SELECT COUNT(DISTINCT operator_id)
      FROM (SELECT DISTINCT operator_id
              FROM monthly_network_triplets_per_mno
             WHERE imei_norm = imei
               AND last_seen >= analysis_start_date::date
               AND first_seen < analysis_end_date::date) t
$$;
