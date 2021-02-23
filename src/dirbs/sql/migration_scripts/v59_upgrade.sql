--
-- DIRBS SQL migration script (v58 -> v59)
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
-- This fix is required to handle NULL most_recent_add_remove_reason
--
-- this drop DROP AGGREGATE overall_delta_reason(reason TEXT) but needs CASCADE;
DROP FUNCTION pick_delta_reason(state delta_reason_state) CASCADE;

CREATE FUNCTION pick_delta_reason(state delta_reason_state)
    RETURNS TEXT
    LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE
    AS $$
BEGIN
    --
    -- 1. If net_adds is non_zero, return the most recent add or remove reason
    --           -> ('change', 'add', 'remove', 'add') should return 'add' rather than change
    --           -> ('change', 'add') should return 'add' rather than change
    --           -> ('remove', add', 'remove', 'change') should return 'remove'
    IF state.net_adds != 0 THEN
        RETURN state.most_recent_add_remove_reason;
    END IF;

    --
    -- 2. Else if there was no change reason seen, return NULL
    --           -> ('add', 'remove') should return NULL
    --           -> ('remove', 'add', 'remove', 'add') should return NULL
    --
    IF NOT state.has_change_reason THEN
        RETURN NULL;
    END IF;

    --
    -- 3. Else if there was a change, and the last add or remove reason was a add, return 'change'
    --           -> ('change', 'add', 'remove') should return 'change'
    --           -> ('add', 'remove', 'change') should return 'change'
    --
    IF state.most_recent_add_remove_reason IS NULL OR
       state.most_recent_add_remove_reason IN ('new', 'blocked', 'added') THEN
        RETURN 'change';
    END IF;

    --
    -- 4. Else return NULL
    --           -> ('remove', 'change', 'add') should return NULL
    --
    RETURN NULL;
END
$$;

--
-- This fix will throw an exception for Base Run ID greater than Run ID
--
DROP FUNCTION gen_delta_blacklist(base_run_id BIGINT, run_id BIGINT);
DROP FUNCTION gen_delta_notifications_list(op_id TEXT, base_run_id BIGINT, run_id BIGINT);
DROP FUNCTION gen_delta_exceptions_list(op_id TEXT, base_run_id BIGINT, run_id BIGINT);

CREATE AGGREGATE overall_delta_reason(reason TEXT) (
    sfunc               = process_next_reason,
    stype               = delta_reason_state,
    finalfunc           = pick_delta_reason,
    initcond            = '(0, FALSE,)'
);

CREATE FUNCTION gen_delta_blacklist(base_run_id BIGINT, run_id BIGINT = -1)
    RETURNS TABLE (
        imei_norm       TEXT,
        block_date      DATE,
        reasons         TEXT[],
        delta_reason    TEXT
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
    --
    -- We need to process all changes in the range and use aggregates to pick the best delta reason.
    -- If the an IMEI was already on the blacklist but the latest change was 'changed', we still need to return
    -- 'added'.
    --
    RETURN QUERY SELECT *
                   FROM (SELECT bl.imei_norm,
                                first(bl.block_date ORDER BY start_run_id DESC) AS block_date,
                                first(bl.reasons ORDER BY start_run_id DESC) AS reasons,
                                overall_delta_reason(bl.delta_reason ORDER BY start_run_id DESC) AS delta_reason
                           FROM blacklist bl
                          WHERE start_run_id > base_run_id
                            AND start_run_id <= run_id
                       GROUP BY bl.imei_norm) x
                  WHERE x.delta_reason IS NOT NULL;
END
$$;


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
        delta_reason    TEXT
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
                                overall_delta_reason(nl.delta_reason ORDER BY start_run_id DESC) AS delta_reason
                           FROM notifications_lists nl
                          WHERE operator_id = op_id
                            AND start_run_id > base_run_id
                            AND start_run_id <= run_id
                       GROUP BY nl.imei_norm, nl.imsi, nl.msisdn) x
                  WHERE x.delta_reason IS NOT NULL;
END
$$;


--
-- Create function to generate a per-MNO delta exceptions list for a run_id, operator id and optional base_run_id.
--
-- If not base_run_id is supplied, this function will use the maximum run_id found in the DB that it less than
-- than the supplied run_id
--
CREATE FUNCTION gen_delta_exceptions_list(op_id TEXT, base_run_id BIGINT, run_id BIGINT = -1)
    RETURNS TABLE (
        imei_norm       TEXT,
        imsi            TEXT,
        delta_reason    TEXT
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
                   FROM (SELECT el.imei_norm,
                                el.imsi,
                                overall_delta_reason(el.delta_reason ORDER BY start_run_id DESC) AS delta_reason
                           FROM exceptions_lists el
                          WHERE operator_id = op_id
                            AND start_run_id > base_run_id
                            AND start_run_id <= run_id
                       GROUP BY el.imei_norm, el.imsi) x
                  WHERE x.delta_reason IS NOT NULL;
END
$$;
