--
-- DIRBS SQL migration script (v68 -> v69)
--
-- Copyright (c) 2018-2019 Qualcomm Technologies, Inc.
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
--   If you use this software in a product, an acknowledgment is required by displaying the trademark/log as per the
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
-- This script is almost the same as v64_upgrade.sql, which was modified from its original code because
-- upgrades were failing due to an inherited constraint that was not removed from child tables
-- when it was removed from the parent. The only different is the IF EXISTS in the DROP CONSTRAINT for children,
-- as this constraint will not exist if the modified v64_upgrade.sql has run. It also normalizes the constraints
-- for exception lists
--
-- We run this script again as a v69 script to to ensure that installations that were on v64+ of the schema
-- before the fix was made have the same exact schema as installations that have only run the fixed
-- v64 migration script
--
ALTER TABLE notifications_lists DROP CONSTRAINT notifications_lists_delta_reason_check;

DO $$
DECLARE
    tbl_name TEXT;
BEGIN
    FOR tbl_name IN
        SELECT c.relname
          FROM pg_inherits
          JOIN pg_class AS c ON (inhrelid=c.oid)
          JOIN pg_class as p ON (inhparent=p.oid)
          JOIN pg_namespace pn ON pn.oid = p.relnamespace
          JOIN pg_namespace cn ON cn.oid = c.relnamespace
         WHERE p.relname = 'notifications_lists' and pn.nspname = 'core'
    LOOP
        EXECUTE 'ALTER TABLE '
            || quote_ident(tbl_name)
            || ' DROP CONSTRAINT IF EXISTS notifications_lists_delta_reason_check' USING tbl_name;
    END LOOP;
END $$;

ALTER TABLE notifications_lists
    ADD CONSTRAINT notifications_lists_delta_reason_check CHECK (delta_reason IN ('new',
                                                                                  'resolved',
                                                                                  'blacklisted',
                                                                                  'no_longer_seen',
                                                                                  'changed'));

ALTER TABLE exceptions_lists DROP CONSTRAINT exceptions_lists_delta_reason_check;

DO $$
DECLARE
    tbl_name TEXT;
BEGIN
    FOR tbl_name IN
        SELECT c.relname
          FROM pg_inherits
          JOIN pg_class AS c ON (inhrelid=c.oid)
          JOIN pg_class as p ON (inhparent=p.oid)
          JOIN pg_namespace pn ON pn.oid = p.relnamespace
          JOIN pg_namespace cn ON cn.oid = c.relnamespace
         WHERE p.relname = 'exceptions_lists' and pn.nspname = 'core'
    LOOP
        EXECUTE 'ALTER TABLE '
            || quote_ident(tbl_name)
            || ' DROP CONSTRAINT IF EXISTS exceptions_lists_delta_reason_check' USING tbl_name;
    END LOOP;
END $$;

ALTER TABLE exceptions_lists
    ADD CONSTRAINT exceptions_lists_delta_reason_check CHECK (delta_reason IN ('added', 'removed'));
