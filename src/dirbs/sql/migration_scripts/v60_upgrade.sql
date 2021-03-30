--
-- DIRBS SQL migration script (v59 -> v60)
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
-- This fix is meant to return 'changed' instead of 'change'
--
DROP FUNCTION pick_delta_reason(state delta_reason_state) CASCADE;

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
        RETURN 'changed';
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
