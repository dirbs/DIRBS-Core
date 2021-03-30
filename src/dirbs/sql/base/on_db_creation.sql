--
-- Template script for new PostgreSQL databases, used by dirbs-db create
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

-- Create a new schema that is owned by dirbs_core_base
CREATE SCHEMA core AUTHORIZATION dirbs_core_power_user;

-- Grant usage to all dirbs_core_base users
GRANT USAGE ON SCHEMA core TO dirbs_core_base;

-- Grant create to all importers
DO $$
DECLARE
    import_roles TEXT[] = ARRAY['dirbs_core_import_operator', 'dirbs_core_import_gsma',
                                'dirbs_core_import_registration_list', 'dirbs_core_import_stolen_list',
                                'dirbs_core_import_pairing_list', 'dirbs_core_import_golden_list',
                                'dirbs_core_listgen', 'dirbs_core_import_barred_list',
                                'dirbs_core_import_barred_tac_list', 'dirbs_core_import_subscribers_registration_list',
                                'dirbs_core_import_device_association_list', 'dirbs_core_import_monitoring_list'];
    import_role TEXT;
BEGIN
    FOREACH import_role IN ARRAY import_roles
    LOOP
        EXECUTE 'GRANT CREATE ON SCHEMA core TO ' || import_role;
    END LOOP;
END $$;

DO $$
DECLARE
    database_name TEXT;
BEGIN
    SELECT current_database() INTO database_name;
    -- Set the ownership of the current database to dirbs_core_power_user
    EXECUTE 'ALTER DATABASE ' || quote_ident(database_name) || ' OWNER TO dirbs_core_power_user';
    -- Set the search path of this database to "core"
    EXECUTE 'ALTER DATABASE ' || quote_ident(database_name) || ' SET search_path TO core';
END $$;

-- Modify the search_path in the current session as well
SET search_path TO core;
