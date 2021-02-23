--
-- DIRBS SQL migration script (v62 -> v63)
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
-- Function that returns the most relevant delta reason from the list for a triplet. Relies on the aggregate
-- being called with ORDER BY DESC start_run_id to ensure that reasons are processed in the right order (most
-- recent delta reason first.
--
DROP FUNCTION process_next_reason(delta_reason_state, TEXT) CASCADE;

CREATE FUNCTION process_next_reason(state delta_reason_state, reason TEXT)
    RETURNS delta_reason_state
    LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE
    AS $$
DECLARE
    add_reasons         TEXT[];
    remove_reasons      TEXT[];
    change_reasons      TEXT[];
    is_add_reason       BOOLEAN;
    is_change_reason    BOOLEAN;
    is_remove_reason    BOOLEAN;
BEGIN
    add_reasons := ARRAY['new', 'blocked', 'added'];
    remove_reasons := ARRAY['resolved', 'blacklisted', 'removed', 'unblocked', 'no_longer_seen'];
    change_reasons := ARRAY['changed'];
    is_change_reason := (reason = ANY(change_reasons));
    is_add_reason := (reason = ANY(add_reasons));
    is_remove_reason := (reason = ANY(remove_reasons));
    ASSERT is_change_reason OR is_add_reason OR is_remove_reason,
        'Unknown reason "' || reason || '" - not add, remove or change type!';

    IF is_change_reason THEN
        state.has_change_reason := TRUE;
    END IF;

    IF NOT is_change_reason AND state.most_recent_add_remove_reason IS NULL THEN
        state.most_recent_add_remove_reason := reason;
    END IF;

    IF is_add_reason THEN
        state.net_adds := state.net_adds + 1;
    ELSIF is_remove_reason THEN
        state.net_adds := state.net_adds - 1;
    END IF;

    --
    -- DIRBS-960: Add sanity check
    --
    IF state.most_recent_add_remove_reason = ANY(add_reasons) THEN
        ASSERT state.net_adds BETWEEN 0 AND 1, 'Multiple add reasons in a row - should not happen!';
    ELSIF state.most_recent_add_remove_reason = ANY(remove_reasons) THEN
        ASSERT state.net_adds BETWEEN -1 AND 0, 'Multiple remove reasons in a row - should not happen!';
    ELSE
        ASSERT state.net_adds = 0, 'No add or remove reasons, yet net_adds non-zero - should not happen!';
    END IF;

    RETURN state;
END
$$;

--
-- Aggregate wrapper around compare_delta_reasons.
--
CREATE AGGREGATE overall_delta_reason(reason TEXT) (
    sfunc               = process_next_reason,
    stype               = delta_reason_state,
    finalfunc           = pick_delta_reason,
    initcond            = '(0, FALSE,)'
);
