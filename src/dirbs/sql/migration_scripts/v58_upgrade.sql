--
-- DIRBS SQL migration script (v57 -> v58)
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
-- Remove existing tables
--
DROP TABLE blacklist;
DROP TABLE notification_lists;
DROP TABLE exception_lists;

--
-- Make sure listgen can create tables
--
GRANT CREATE ON SCHEMA core TO dirbs_core_listgen;

--
-- Create new blacklist table (delta storage, non-partitioned)
--
CREATE TABLE blacklist (
    row_id          BIGSERIAL NOT NULL,
    imei_norm       TEXT NOT NULL,
    block_date      DATE NOT NULL,
    reasons         TEXT[] NOT NULL,
    start_run_id    BIGINT NOT NULL,
    end_run_id      BIGINT,
    delta_reason    TEXT NOT NULL CHECK (delta_reason IN ('blocked', 'unblocked', 'changed'))
) WITH (fillfactor = 45);
ALTER TABLE blacklist OWNER TO dirbs_core_listgen;
CREATE INDEX ON blacklist USING btree(start_run_id);
CREATE INDEX ON blacklist USING btree(end_run_id);
CREATE UNIQUE INDEX ON blacklist USING btree (imei_norm) WHERE (end_run_id IS NULL);

--
-- Create generic trigger to prevent accidental insertion on parent table
--
CREATE FUNCTION fail_on_generic_parent_table_insert() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    RAISE EXCEPTION 'Trying to insert directly into parent table! Insert into child table directly instead.';
    RETURN NULL;
END
$$;

CREATE TABLE notifications_lists (
    row_id          BIGINT NOT NULL,
    operator_id     TEXT NOT NULL,
    imei_norm       TEXT NOT NULL,
    imsi            TEXT NOT NULL,
    msisdn          TEXT NOT NULL,
    block_date      DATE NOT NULL,
    reasons         TEXT[] NOT NULL,
    start_run_id    BIGINT NOT NULL,
    end_run_id      BIGINT,
    delta_reason    TEXT NOT NULL CHECK (delta_reason IN ('new', 'resolved', 'blacklisted', 'changed'))
);
ALTER TABLE notifications_lists OWNER TO dirbs_core_listgen;
CREATE TRIGGER notifications_lists_trigger BEFORE INSERT ON notifications_lists
    FOR EACH ROW EXECUTE PROCEDURE fail_on_generic_parent_table_insert();

--
-- Create new exception_lists table (delta storage, partitioned by operator)
--
CREATE TABLE exceptions_lists (
    row_id          BIGINT NOT NULL,
    operator_id     TEXT NOT NULL,
    imei_norm       TEXT NOT NULL,
    imsi            TEXT NOT NULL,
    start_run_id    BIGINT NOT NULL,
    end_run_id      BIGINT,
    delta_reason    TEXT NOT NULL CHECK (delta_reason IN ('added', 'removed'))
);
ALTER TABLE exceptions_lists OWNER TO dirbs_core_listgen;
CREATE TRIGGER exceptions_lists_trigger BEFORE INSERT ON exceptions_lists
    FOR EACH ROW EXECUTE PROCEDURE fail_on_generic_parent_table_insert();

--
-- Create function to generate the check digit for a given id_number (string
-- of numbers). Assumes that the check digit has not already been added.
-- Also does not handle non-numeric characters -- function will simply error
--
CREATE FUNCTION luhn_check_digit_generate(id_number TEXT) RETURNS SMALLINT
    LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE
    AS $$
DECLARE
    odd_digits_sum  INTEGER;
    even_digits_sum INTEGER;
    chars           TEXT[];
    nchars          INTEGER;
    temp            INTEGER;
BEGIN
    chars := string_to_array(id_number, NULL);
    nchars := array_length(chars, 1);
    IF nchars IS NULL THEN
        RAISE EXCEPTION 'id_number must be at least one digit';
    END IF;

    even_digits_sum := 0;
    FOR i IN REVERSE nchars..1 BY 2 LOOP
        temp := 2 * chars[i]::INTEGER;
        even_digits_sum := even_digits_sum + temp / 10 + temp % 10;
    END LOOP;

    odd_digits_sum := 0;
    FOR i IN REVERSE nchars-1..1 BY 2 LOOP
        odd_digits_sum := odd_digits_sum + chars[i]::INTEGER;
    END LOOP;

    RETURN (10 - (odd_digits_sum + even_digits_sum) % 10) % 10;
END
$$;

--
-- Create function to verify the check digit for an ID number.
--
CREATE FUNCTION luhn_check_digit_verify(id_number TEXT) RETURNS BOOLEAN
    LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE
    AS $$
DECLARE
    id_number_no_check_digit    TEXT;
    check_digit                 SMALLINT;
BEGIN
    IF length(id_number) < 2 THEN
        RAISE EXCEPTION 'id_number must be at least one digit + check digit';
    END IF;
    check_digit = RIGHT(id_number, 1)::SMALLINT;
    id_number_no_check_digit = LEFT(id_number, -1);

    RETURN check_digit = luhn_check_digit_generate(id_number_no_check_digit);
END
$$;

--
-- Create function to verify the check digit for an ID number.
--
CREATE FUNCTION luhn_check_digit_append(id_number TEXT) RETURNS TEXT
    LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE
    AS $$
BEGIN
    RETURN id_number || luhn_check_digit_generate(id_number);
END
$$;

--
-- Function to work out whether an imei_norm is valid or not
--
CREATE FUNCTION is_valid_imei_norm(imei_norm TEXT)
    RETURNS BOOLEAN
    LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE
    AS $$
    SELECT imei_norm ~ '^\d{14}$';
$$;

--
-- Function to calculate the maximum value of a BIGINT
--
CREATE FUNCTION max_bigint()
    RETURNS BIGINT
    LANGUAGE sql STRICT IMMUTABLE PARALLEL SAFE
    AS $$
    SELECT -1 * (((2 ^ (8 * pg_column_size(1::bigint) - 2))::bigint << 1) + 1)
$$;

--
-- Create function to generate a full blacklist for a given run_id. A value of -1 means get the latest list.
--
CREATE FUNCTION gen_blacklist(run_id BIGINT = -1)
    RETURNS TABLE (
        imei_norm   TEXT,
        block_date  DATE,
        reasons     TEXT[]
    )
    LANGUAGE plpgsql STRICT STABLE PARALLEL SAFE
    AS $$
DECLARE
    query_run_id    BIGINT;
BEGIN
    --
    -- If we don't specify a run_id, just set to the maximum run_id which will always return all rows where
    -- end_run_id is NULL
    --
    IF run_id = -1 THEN
        run_id := max_bigint();
    END IF;

    RETURN QUERY SELECT bl.imei_norm,
                        bl.block_date,
                        bl.reasons
                   FROM blacklist bl
                  WHERE bl.delta_reason != 'unblocked'
                    AND run_id >= bl.start_run_id
                    AND (run_id < bl.end_run_id OR bl.end_run_id IS NULL);
END
$$;

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
        reasons         TEXT[]
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
                        nl.reasons
                   FROM notifications_lists nl
                  WHERE nl.operator_id = op_id
                    AND nl.delta_reason NOT IN ('resolved', 'blacklisted')
                    AND run_id >= nl.start_run_id
                    AND (run_id < nl.end_run_id OR nl.end_run_id IS NULL);
END
$$;

--
-- Create function to generate a full exceptions_list for a given run_id and operator ID. A value
-- of -1 means get the latest list.
--
CREATE FUNCTION gen_exceptions_list(op_id TEXT, run_id BIGINT = -1)
    RETURNS TABLE (
        imei_norm   TEXT,
        imsi        TEXT
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

    RETURN QUERY SELECT el.imei_norm,
                        el.imsi
                   FROM exceptions_lists el
                  WHERE el.operator_id = op_id
                    AND el.delta_reason != 'removed'
                    AND run_id >= el.start_run_id
                    AND (run_id < el.end_run_id OR el.end_run_id IS NULL);
END
$$;

--
-- Simple function that simple returns the first element out of any 2
--
CREATE FUNCTION first_element(first ANYELEMENT, second ANYELEMENT)
    RETURNS anyelement
    LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE
    AS $$
    SELECT first;
$$;

--
-- First aggregate to return the first element for a group. Marked PARALLEL UNSAFE due to dependence on order.
--
CREATE AGGREGATE first(
    SFUNC    = first_element,
    BASETYPE = ANYELEMENT,
    STYPE    = ANYELEMENT
);

--
-- If we are doing deltas across multiple versions, we check
--   1. How many net 'added' types we have seen during the window (adds are +1, removes are -1)
--   2. The most recent reason
--   3. The least recent reason
--   4. The most recent reason that removed or added the group from the list
--
-- See the finalize function to see how this state is resolved into an aggregate value
--
CREATE TYPE delta_reason_state AS (
    net_adds                        BIGINT,
    has_change_reason               BOOLEAN,
    most_recent_add_remove_reason   TEXT
);

--
-- Function that returns the most relevant delta reason from the list for a triplet. Relies on the aggregate
-- being called with ORDER BY DESC start_run_id to ensure that reasons are processed in the right order (most
-- recent delta reason first.
--
CREATE FUNCTION process_next_reason(state delta_reason_state, reason TEXT)
    RETURNS delta_reason_state
    LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE
    AS $$
BEGIN
    IF reason = 'changed' THEN
        state.has_change_reason := TRUE;
    END IF;

    IF reason != 'changed' AND state.most_recent_add_remove_reason IS NULL THEN
        state.most_recent_add_remove_reason := reason;
    END IF;

    IF reason IN ('new', 'blocked', 'added') THEN
        state.net_adds := state.net_adds + 1;
    ELSIF reason IN ('resolved', 'blacklisted', 'removed', 'unblocked') THEN
        state.net_adds := state.net_adds - 1;
    END IF;

    RETURN state;
END
$$;

--
-- Take the state and decides what the most relevant change is
--
-- For reference, where comments say 'added', reason is one of: 'new', 'blocked', 'added'
-- For reference, where comments say 'removed', reason is one of: 'resolved', 'blacklisted', 'removed', 'unblocked'
--
CREATE FUNCTION pick_delta_reason(state delta_reason_state)
    RETURNS TEXT
    LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE
    AS $$
BEGIN
    --
    -- 1. If net_adds is non_zero, return the most recent add or remove reason
    --           -> ('changed', 'added', 'removed', 'added') should return 'added' rather than change
    --           -> ('changed', 'added') should return 'added' rather than changed
    --           -> ('removed', added', 'removed', 'changed') should return 'removed'
    IF state.net_adds != 0 THEN
        RETURN state.most_recent_add_remove_reason;
    END IF;

    --
    -- 2. Else if there was no change reason seen, return NULL
    --           -> ('added', 'removed') should return NULL
    --           -> ('removed', 'added', 'removed', 'added') should return NULL
    --
    IF NOT state.has_change_reason THEN
        RETURN NULL;
    END IF;

    --
    -- 3. Else if there was a change, and the last add or remove reason was a add, return 'changed'
    --           -> ('changed', 'added', 'removed') should return 'change'
    --           -> ('added', 'removed', 'changed') should return 'change'
    --
    IF state.most_recent_add_remove_reason IS NULL OR
       state.most_recent_add_remove_reason IN ('new', 'blocked', 'added') THEN
        RETURN 'change';
    END IF;

    --
    -- 4. Else return NULL
    --           -> ('removed', 'changed', 'added') should return NULL
    --
    RETURN NULL;
END
$$;

--
-- Aggregate wrapper around compare_delta_reasons. Marked PARALLEL UNSAFE due to dependence on order.
--
CREATE AGGREGATE overall_delta_reason(reason TEXT) (
    sfunc               = process_next_reason,
    stype               = delta_reason_state,
    finalfunc           = pick_delta_reason,
    initcond            = '(0, FALSE,)'
);

--
-- Create function to generate a delta blacklist between 2 run_ids
--
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
