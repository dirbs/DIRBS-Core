--NetVision data extract for DIRBS
--
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
--This version takes all the 5 months data for one operator = currently written as ETB, mapping to Zong
--Note due to subtle differences in the datasets, can't just replace the table names with other Colombian operators - need to examine how data was uploaded
--remember to set the $mylimit variable to ALL when you are ready to do all the data!

--Source Tables - currently only ETB
--NV_TABLE_DECLARE_BEGIN
--NV_TABLE_DECLARE_TABLE = $etb_511,NV_COLOMBIA_MOVILE_SA_ESP_QTLBASICRAW_ROBERTT_20160107_150639357
--NV_TABLE_DECLARE_TABLE = $etb_512,NV_COLOMBIA_MOVILE_SA_ESP_QTLBASICRAW_TOMASG_20160210_120047842
--NV_TABLE_DECLARE_TABLE = $etb_601,NV_ETB_QTLCALLRECORDS_OP_DSALEK_20160408_145007098
--NV_TABLE_DECLARE_TABLE = $etb_602,NV_ETB_QTLCALLRECORDS_OP_DSALEK_20160422_094202927
--NV_TABLE_DECLARE_TABLE = $etb_603,NV_ETB_QTLCALLRECORDS_OP_DSALEK_20160512_221123656
--NV_TABLE_DECLARE_END


--Config Variables
--NV_VARIABLE_DECLARE_BEGIN
--NV_VARIABLE_DECLARE_VARIABLE = $existing_plmnid,string,'73218'
--NV_VARIABLE_DECLARE_VARIABLE = $new_plmnid,string,'41004'
--NV_VARIABLE_DECLARE_VARIABLE = $msisdn_pre,string,'92'
--NV_VARIABLE_DECLARE_VARIABLE = $mylimit,string,100
--NV_VARIABLE_DECLARE_END 

--log outputs table
create temp table loggy (
myindex int
,descr varchar(80)
,num int
,mycomment varchar(100)
)distribute on random;

insert into loggy (myindex, descr, num, mycomment) select 100, 'total count 2015-11', count(*), NULL from $etb_511;
insert into loggy (myindex, descr, num, mycomment) select 200, 'total count 2015-12', count(*), NULL from $etb_512;
insert into loggy (myindex, descr, num, mycomment) select 300, 'total count 2016-01', count(*), NULL from $etb_601;
insert into loggy (myindex, descr, num, mycomment) select 400, 'total count 2016-02', count(*), NULL from $etb_602;
insert into loggy (myindex, descr, num, mycomment) select 500, 'total count 2016-03', count(*), NULL from $etb_603;




--do export
--try to preserve some interest in the data by only rewriting MSISDN/IMSI prefix if matches the original home operator
--for ETB, there happen to be no such IMSIs

--Nov data has date_logged set to a single date, so use fetch_datE_time instead
create temp table op1 as

(
select distinct
	to_char(to_date(fetch_date_time, 'YYYY-MM-DD HH:MI:SS'),'YYYYMMDD') date
	,imei
	,case
		when substr(imsi,1,length($existing_plmnid)) = $existing_plmnid then $new_plmnid || substr(imsi,length($existing_plmnid)+1,length(imsi)-length($existing_plmnid)) 
		else IMSI
	end imsi
	,case
		when substr(imsi,1,length($existing_plmnid)) = $existing_plmnid  then $msisdn_pre || substr(imsi, length($msisdn_pre)+1, length(imsi)-length($msisdn_pre))
		else imsi
	end msisdn
from $etb_511
limit $mylimit
) distribute on random;


--dec data has date_logged as a date, not a varchar
create temp table op2 as
(
select distinct
	to_char(date_logged, 'YYYYMMDD') date
	,imei
	,case
		when substr(imsi,1,length($existing_plmnid)) = $existing_plmnid then $new_plmnid || substr(imsi,length($existing_plmnid)+1,length(imsi)-length($existing_plmnid)) 
		else IMSI
	end imsi
	,case
		when substr(imsi,1,length($existing_plmnid)) = $existing_plmnid  then $msisdn_pre || substr(imsi, length($msisdn_pre)+1, length(imsi)-length($msisdn_pre))
		else imsi
	end msisdn
from $etb_512
limit $mylimit
) distribute on random;

--2016 Jan/Feb/Mar has a row with IMSI all dashes - exclude by null date_logged
create temp table op3 as
(
select distinct
	date_logged date
	,imei
	,case
		when substr(imsi,1,length($existing_plmnid)) = $existing_plmnid then $new_plmnid || substr(imsi,length($existing_plmnid)+1,length(imsi)-length($existing_plmnid)) 
		else IMSI
	end imsi
	,case
		when substr(imsi,1,length($existing_plmnid)) = $existing_plmnid  then $msisdn_pre || substr(imsi, length($msisdn_pre)+1, length(imsi)-length($msisdn_pre))
		else imsi
	end msisdn
from $etb_601
where date_logged is not null
limit $mylimit
) distribute on random;


create temp table op4 as
(
select distinct
	date_logged date
	,imei
	,case
		when substr(imsi,1,length($existing_plmnid)) = $existing_plmnid then $new_plmnid || substr(imsi,length($existing_plmnid)+1,length(imsi)-length($existing_plmnid)) 
		else IMSI
	end imsi
	,case
		when substr(imsi,1,length($existing_plmnid)) = $existing_plmnid  then $msisdn_pre || substr(imsi, length($msisdn_pre)+1, length(imsi)-length($msisdn_pre))
		else imsi
	end msisdn
from $etb_602
where date_logged is not null
limit $mylimit
) distribute on random;

create temp table op5 as
(
select distinct
	date_logged date
	,imei
	,case
		when substr(imsi,1,length($existing_plmnid)) = $existing_plmnid then $new_plmnid || substr(imsi,length($existing_plmnid)+1,length(imsi)-length($existing_plmnid)) 
		else IMSI
	end imsi
	,case
		when substr(imsi,1,length($existing_plmnid)) = $existing_plmnid  then $msisdn_pre || substr(imsi, length($msisdn_pre)+1, length(imsi)-length($msisdn_pre))
		else imsi
	end msisdn
from $etb_603
where date_logged is not null
limit $mylimit
) distribute on random;

insert into loggy (myindex, descr, num, mycomment) select 600, 'distinct count 2015-11', count(*), NULL from op1;
insert into loggy (myindex, descr, num, mycomment) select 700, 'distinct count 2015-12', count(*), NULL from op2;
insert into loggy (myindex, descr, num, mycomment) select 800, 'distinct count 2016-01', count(*), NULL from op3;
insert into loggy (myindex, descr, num, mycomment) select 900, 'distinct count 2016-02', count(*), NULL from op4;
insert into loggy (myindex, descr, num, mycomment) select 1000, 'distinct count 2016-03', count(*), NULL from op5;

--compute null counts, to compare with DIRBS import checks
insert into loggy (myindex, descr, num, mycomment) 
select 
	1100
	,'count any null in triplet 2015-11'
	,count(*)
	,NULL
from 
	op1
where 
	imsi = '' or imsi is null
	or imei = '' or imei is null
	or msisdn = '' or msisdn is null
;

insert into loggy (myindex, descr, num, mycomment) 
select 
	1200
	,'count any null in triplet 2015-12'
	,count(*)
	,NULL
from 
	op2
where 
	imsi = '' or imsi is null
	or imei = '' or imei is null
	or msisdn = '' or msisdn is null
;
insert into loggy (myindex, descr, num, mycomment) 
select 
	1300
	,'count any null in triplet 2016-01'
	,count(*)
	,NULL
from 
	op3
where 
	imsi = '' or imsi is null
	or imei = '' or imei is null
	or msisdn = '' or msisdn is null
;
insert into loggy (myindex, descr, num, mycomment) 
select 
	1400
	,'count any null in triplet 2016-02'
	,count(*)
	,NULL
from 
	op4
where 
	imsi = '' or imsi is null
	or imei = '' or imei is null
	or msisdn = '' or msisdn is null
;
insert into loggy (myindex, descr, num, mycomment) 
select 
	1500
	,'count any null in triplet 2016-03'
	,count(*)
	,NULL
from 
	op5
where 
	imsi = '' or imsi is null
	or imei = '' or imei is null
	or msisdn = '' or msisdn is null
;



select myindex, descr,num, mycomment from loggy order by myindex;

--has already loaded OK, so no additional filtering step here
select * from op1;
select * from op2;
select * from op3;
select * from op4;
select * from op5;
