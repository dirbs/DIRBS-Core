--NetVision data extract for DIRBS
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
--
--
--Reuse CRC data to generate dummy operator input dump for DIRBS

--Source Tables - currently using 2016-03 for all operators
--NV_TABLE_DECLARE_BEGIN
--NV_TABLE_DECLARE_TABLE = $av_d,NV_AVANTEL_QTLCALLRECORDS_OP_TOMASG_20160512_163614890
--NV_TABLE_DECLARE_TABLE = $cl_d,NV_CLARO_COLOMBIA_QTLCALLRECORDS_OP_TOMASG_20160512_182039485
--NV_TABLE_DECLARE_TABLE = $cl_v,NV_CLARO_COLOMBIA_QTLCALLRECORDS_OP_DSALEK_20160516_125550384
--NV_TABLE_DECLARE_TABLE = $et_d,NV_ETB_QTLCALLRECORDS_OP_DSALEK_20160512_221123656
--NV_TABLE_DECLARE_TABLE = $tf_d,NV_TELEFONICA_COLOMBIA_QTLCALLRECORDS_OP_DSALEK_20160513_123731592
--NV_TABLE_DECLARE_TABLE = $tf_v,NV_TELEFONICA_COLOMBIA_QTLCALLRECORDS_OP_DSALEK_20160513_081406011
--NV_TABLE_DECLARE_TABLE = $tg_vd,NV_TIGO_MILLICOM_COLOMBIA_QTLCALLRECORDS_OP_DSALEK_20160515_001119813
--NV_TABLE_DECLARE_END


--Config Variables
--NV_VARIABLE_DECLARE_BEGIN
--NV_VARIABLE_DECLARE_VARIABLE = $op_name,string,'Zong'
--NV_VARIABLE_DECLARE_VARIABLE = $msisdn_pre,string,'92'
--NV_VARIABLE_DECLARE_VARIABLE = $mylimit,string,1000
--NV_VARIABLE_DECLARE_END 

--log outputs table
create temp table loggy (
myindex int
,descr varchar(80)
,num int
,mycomment varchar(100)
)distribute on random;



--make big table of combined operator data
create temp table op_combined as
(
	(select imsi, imei, date_logged from $av_d limit $mylimit)
	union (select substring(imsi,2), imei, date_logged from $cl_d limit $mylimit)
	union (select substring(imsi,2), imei, date_logged from $cl_v limit $mylimit) 
	union (select imsi, imei, date_logged from $et_d limit $mylimit)
	union (select imsi, imei, date_logged from $tf_d limit $mylimit)
	union (select imsi, imei, date_logged from $tf_v limit $mylimit)
	union (select imsi, imei, date_logged from $tg_vd limit $mylimit)
) distribute on random;

insert into loggy (myindex, descr, num, mycomment) select 100, 'distinct entries all operators', count(*), NULL from op_combined;
insert into loggy (myindex, descr, num, mycomment) select 200, 'distinct days', count(distinct date_logged), 'should be 31' from op_combined;
insert into loggy (myindex, descr, num, mycomment) select 300, 'distinct imeis', count(distinct imei), 'should be ~50M?' from op_combined;

--do export
--just prefix a country code to the IMSI to make a dummy MSISDN

select myindex, descr,num, mycomment from loggy order by myindex;

--do export
--just prefix a country code to the IMSI to make a dummy MSISDN - simpler than trying to remove MCC first
select 
	$op_name operator_name
	,date_logged date
	,imei
	,imsi
	,$msisdn_pre || imsi msisdn
from op_combined;

