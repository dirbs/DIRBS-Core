--
-- Template script to create abstract roles for DIRBS Core
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

DO $$
DECLARE
    roles TEXT[] = ARRAY['dirbs_core_job', 'dirbs_core_import_operator', 'dirbs_core_import_gsma',
                         'dirbs_core_import_registration_list', 'dirbs_core_import_stolen_list',
                         'dirbs_core_import_pairing_list', 'dirbs_core_import_golden_list', 'dirbs_core_classify',
                         'dirbs_core_listgen', 'dirbs_core_report', 'dirbs_core_catalog', 'dirbs_core_api',
                         'dirbs_core_import_barred_list', 'dirbs_core_import_barred_tac_list',
                         'dirbs_core_import_subscribers_registration_list', 'dirbs_core_white_list',
                         'dirbs_core_import_device_association_list', 'dirbs_core_import_monitoring_list'];
    role TEXT;
BEGIN
    -- Create base role
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'dirbs_core_base') THEN
        CREATE ROLE dirbs_core_base NOLOGIN;
    END IF;

    -- Create power user role
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'dirbs_core_power_user') THEN
        CREATE ROLE dirbs_core_power_user NOLOGIN;
    END IF;
    GRANT dirbs_core_base TO dirbs_core_power_user;

    FOREACH role IN ARRAY roles
    LOOP
        IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = role) THEN
            -- Create role
            EXECUTE 'CREATE ROLE ' || role || ' NOLOGIN';
        END IF;
        -- Grant base role to this one
        EXECUTE 'GRANT dirbs_core_base TO ' || role;
        -- Grant role to power user
        EXECUTE 'GRANT ' || role || ' TO dirbs_core_power_user';
    END LOOP;

END $$;

-- Grant job roles to every job (no API, no power user, etc. -- power user will inherit this indirectly)
DO $$
DECLARE
    jobs TEXT[] = ARRAY['dirbs_core_import_operator', 'dirbs_core_import_gsma', 'dirbs_core_import_registration_list',
                        'dirbs_core_import_stolen_list', 'dirbs_core_import_pairing_list', 'dirbs_core_white_list',
                        'dirbs_core_import_golden_list', 'dirbs_core_classify', 'dirbs_core_listgen',
                        'dirbs_core_report', 'dirbs_core_catalog', 'dirbs_core_import_barred_list',
                        'dirbs_core_import_barred_tac_list', 'dirbs_core_import_subscribers_registration_list',
                        'dirbs_core_import_device_association_list', 'dirbs_core_import_monitoring_list'];
    job TEXT;
BEGIN
    FOREACH job IN ARRAY jobs
    LOOP
        EXECUTE 'GRANT dirbs_core_job TO ' || job;
    END LOOP;
END $$;
