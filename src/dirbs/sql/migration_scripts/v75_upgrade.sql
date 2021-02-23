--
-- DIRBS SQL migration script (v74 -> v75)
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
-- Add column to classification_state table to track amnesty granted parameter.
--
ALTER TABLE classification_state ADD COLUMN amnesty_granted BOOLEAN DEFAULT FALSE NOT NULL;

--
-- Add column to notification_lists table to track amnesty granted parameter.
--
ALTER TABLE notifications_lists ADD COLUMN amnesty_granted BOOLEAN DEFAULT FALSE NOT NULL;

--
-- Drop function gen_notifications_list to enable return table column to include amnesty_granted column
--
DROP FUNCTION gen_notifications_list(text,bigint);

--
-- Create function to generate a full notifications_list for a given run_id and operator ID. A value
-- of -1 means get the latest list.
--
CREATE FUNCTION gen_notifications_list(op_id TEXT, run_id BIGINT = -1)
    RETURNS TABLE (
        imei_norm       TEXT,
        imsi            TEXT,
        msisdn          TEXT,
        block_date      DATE,
        reasons         TEXT[],
        amnesty_granted BOOLEAN
    )
    LANGUAGE plpgsql STRICT STABLE PARALLEL SAFE
    AS $$
BEGIN
    --
    -- If we don't specify a run_id, just set to the maximum run_id which will always return all rows where
    -- end_run_id is NULL
    --
    IF run_id = -1 THEN
        run_id := max_bigint();
    END IF;

    RETURN QUERY SELECT nl.imei_norm,
                        nl.imsi,
                        nl.msisdn,
                        nl.block_date,
                        nl.reasons,
                        nl.amnesty_granted
                   FROM notifications_lists nl
                  WHERE nl.operator_id = op_id
                    AND nl.delta_reason NOT IN ('resolved', 'blacklisted')
                    AND run_id >= nl.start_run_id
                    AND (run_id < nl.end_run_id OR nl.end_run_id IS NULL);
END
$$;

--
-- Drop function gen_delta_notifications_list to enable return table column to include amnesty_granted column
--
DROP FUNCTION gen_delta_notifications_list(op_id TEXT, base_run_id BIGINT, run_id BIGINT);

--
-- Create function to generate a per-MNO delta notifications list for a run_id, operator id and optional base_run_id.
--
-- If not base_run_id is supplied, this function will use the maximum run_id found in the DB that it less than
-- than the supplied run_id
--
CREATE FUNCTION gen_delta_notifications_list(op_id TEXT, base_run_id BIGINT, run_id BIGINT = -1)
    RETURNS TABLE (
        imei_norm       TEXT,
        imsi            TEXT,
        msisdn          TEXT,
        block_date      DATE,
        reasons         TEXT[],
        delta_reason    TEXT,
        amnesty_granted BOOLEAN
    )
    LANGUAGE plpgsql STRICT STABLE PARALLEL SAFE
    AS $$
BEGIN
    --
    -- If we don't specify a run_id, just set to the maximum run_id
    --
    IF run_id = -1 THEN
        run_id := max_bigint();
    END IF;

    IF run_id < base_run_id THEN
        RAISE EXCEPTION 'Parameter base_run_id % greater than run_id %', base_run_id, run_id;
    END IF;

    RETURN QUERY SELECT *
                   FROM (SELECT nl.imei_norm,
                                nl.imsi,
                                nl.msisdn,
                                first(nl.block_date ORDER BY start_run_id DESC) AS block_date,
                                first(nl.reasons ORDER BY start_run_id DESC) AS reasons,
                                overall_delta_reason(nl.delta_reason ORDER BY start_run_id DESC) AS delta_reason,
                                first(nl.amnesty_granted ORDER BY start_run_id DESC) AS amnesty_granted
                           FROM notifications_lists nl
                          WHERE operator_id = op_id
                            AND start_run_id > base_run_id
                            AND start_run_id <= run_id
                       GROUP BY nl.imei_norm, nl.imsi, nl.msisdn) x
                  WHERE x.delta_reason IS NOT NULL;
END
$$;
