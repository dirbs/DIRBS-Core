--NetVision SQL script for extracting data from GSMA table, in as close to CACF/PTA format as we can get
--still need to clean up after exporting from NVN
--
-- Copyright (c) 2018 Qualcomm Technologies, Inc.
--
-- All rights reserved.
--
-- Redistribution and use in source and binary forms, with or without modification,
-- are permitted (subject to the limitations in the disclaimer below) provided that the
-- following conditions are met:
--
--  * Redistributions of source code must retain the above copyright notice, this list of conditions
--    and the following disclaimer.
--  * Redistributions in binary form must reproduce the above copyright notice, this list of conditions
--    and the following disclaimer in the documentation and/or other materials provided with the distribution.
--  * Neither the name of Qualcomm Technologies, Inc. nor the names of its contributors may be used to endorse
--    or promote products derived from this software without specific prior written permission.
--
-- NO EXPRESS OR IMPLIED LICENSES TO ANY PARTY'S PATENT RIGHTS ARE GRANTED BY THIS LICENSE. THIS SOFTWARE IS PROVIDED
-- BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
-- TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
-- THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
-- CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
-- DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
-- STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE,
-- EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
--

create temp table gsma as 
(
select 
	TAC
	, '"' || replace(GSMA_MARKETING_NAME,'"','""') || '"' MARKETING_NAME
	, '"' || replace(GSMA_INTERNAL_MODEL_NAME,'"','""') || '"' INTERNAL_MODEL_NAME
	, '"' || replace(GSMA_MANUFACTURER,'"','""') || '"' MANUFACTURER
	, '"' || replace(GSMA_BANDS,'"','""') || '"' BANDS
	, to_char(GSMA_ALLOCATION_DATE,'DD-Mon-YYYY') ALLOCATION_DATE
	, '"' || replace(GSMA_COUNTRY_CODE,'"','""') || '"' COUNTRY_CODE
	, '"' || replace(GSMA_FIXED_CODE,'"','""') || '"' FIXED_CODE
	, '"' || replace(GSMA_MANUFACTURER_CODE,'"','""') || '"' MANUFACTURER_CODE
	, '"' || replace(GSMA_RADIO_INTERFACE,'"','""') || '"' RADIO_INTERFACE
	, '"' || replace(GSMA_BRAND_NAME,'"','""') || '"' BRAND_NAME
	, '"' || replace(GSMA_MODEL_NAME,'"','""') || '"' MODEL_NAME
	, '"' || replace(GSMA_OPERATING_SYSTEM,'"','""') || '"' OPERATING_SYSTEM
	, '"' || replace(GSMA_NFC,'"','""') || '"' NFC
	, '"' || replace(GSMA_BLUETOOTH,'"','""') || '"' BLUETOOTH
	, '"' || replace(GSMA_WLAN,'"','""') || '"' WLAN
	, '"' || replace(GSMA_DEVICE_TYPE,'"','""') || '"' DEVICE_TYPE
from NZNVNAGDW..GSMA_MODEL_INFO
) distribute on random;


--pick some particular rows of interest to exercise the formatter:
create temp table with_comma as 
(
	select * from gsma
	where bands like '%,%'
	limit 5
) distribute on random;

create temp table with_doublequote as 
(
	select * from gsma
	where model_name like '%""%'
	limit 5
) distribute on random;

create temp table with_backslash as 
(
	select * from gsma
	where model_name like '%\%' escape ''
	limit 5
) distribute on random;

create temp table end_backslash as 
(
	select * from gsma
	where model_name like '%\' escape ''
	limit 5
) distribute on random;


select * from with_comma
union select * from with_doublequote
union select * from with_backslash
union select * from end_backslash
union select * from gsma where tac in ('01273300','49001841', '35234001');

--Export the sample data we received from CACF
select * from gsma
where tac in ('00100100','00100200', '00100300', '00100400', '00100500', '00100600');

--Export the entire GSMA table
select * from gsma;
