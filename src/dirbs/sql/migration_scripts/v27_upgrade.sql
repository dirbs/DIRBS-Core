--
-- DIRBS SQL migration script (v26 -> v27)
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

CREATE FUNCTION is_valid_subscriber(imsi text) RETURNS boolean
    LANGUAGE sql IMMUTABLE PARALLEL SAFE
    AS $$
    SELECT COALESCE(LENGTH(imsi) BETWEEN 14 AND 15, FALSE);
$$;

-- Zero out the bits for the days that fall outside the analysis window.
-- The analysis end date is non-inclusive so that day is not included.
CREATE FUNCTION get_bitmask_within_window(date_bitmask int, month_first_seen date, month_last_seen date,
                                          analysis_window_start_date date, analysis_window_start_date_dom int,
                                          analysis_window_end_date date, analysis_window_end_date_dom int) RETURNS int
  LANGUAGE sql IMMUTABLE PARALLEL SAFE
    AS $$
    SELECT
           CASE WHEN month_first_seen < analysis_window_start_date AND month_last_seen > analysis_window_end_date
                THEN date_bitmask & ((1 << analysis_window_end_date_dom - 1) - 1) &
                     ~((1 << (analysis_window_start_date_dom) - 1) - 1)
                WHEN month_first_seen < analysis_window_start_date
                THEN date_bitmask & ~((1 << (analysis_window_start_date_dom) - 1) - 1)
                WHEN month_last_seen > analysis_window_end_date
                THEN date_bitmask & ((1 << analysis_window_end_date_dom - 1) - 1)
                ELSE date_bitmask
           END;
$$;
