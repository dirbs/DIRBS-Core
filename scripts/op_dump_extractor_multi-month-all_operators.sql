--NetVision data extract for DIRBS
--Reuse CRC data to generate dummy operator input dump for DIRBS
--This version takes all the 5 months data for all operators, and maps to Mobilink
--Noite this operator has separate voice and data 
--Note due to subtle differences in the datasets, can't just replace the table names with other Colombian operators - need to examine how data was uploaded


--Source Tables 
--NV_TABLE_DECLARE_BEGIN
--Avantel:
--NV_TABLE_DECLARE_TABLE = $s511a,NV_COLOMBIA_MOVILE_SA_ESP_QTLBASICRAW_ROBERTT_20160107_132114743
--NV_TABLE_DECLARE_TABLE = $s512a,NV_COLOMBIA_MOVILE_SA_ESP_QTLBASICRAW_TOMASG_20160208_221914641
--NV_TABLE_DECLARE_TABLE = $s601a,NV_AVANTEL_QTLCALLRECORDS_OP_TOMASG_20160324_111119016
--No avantel feb data as no unique timestamp
--NV_TABLE_DECLARE_TABLE = $s603a,NV_AVANTEL_QTLCALLRECORDS_OP_TOMASG_20160512_163614890
--Claro:
--NV_TABLE_DECLARE_TABLE = $s511c_d,NV_COLOMBIA_MOVILE_SA_ESP_QTLBASICRAW_ROBERTT_20160115_154744788
--NV_TABLE_DECLARE_TABLE = $s512c_d,NV_COLOMBIA_MOVILE_SA_ESP_QTLBASICRAW_TOMASG_20160209_134241322
--NV_TABLE_DECLARE_TABLE = $s601c_d,NV_CLARO_COLOMBIA_QTLCALLRECORDS_OP_TOMASG_20160325_084942238
--NV_TABLE_DECLARE_TABLE = $s602c_d,NV_CLARO_COLOMBIA_QTLCALLRECORDS_OP_TOMASG_20160414_210330064
--NV_TABLE_DECLARE_TABLE = $s603c_d,NV_CLARO_COLOMBIA_QTLCALLRECORDS_OP_TOMASG_20160512_182039485
--NV_TABLE_DECLARE_TABLE = $s511c_v,NV_COLOMBIA_MOVILE_SA_ESP_QTLBASICRAW_ROBERTT_20160107_134604151
--NV_TABLE_DECLARE_TABLE = $s512c_v,NV_COLOMBIA_MOVILE_SA_ESP_QTLBASICRAW_TOMASG_20160210_201235327
--NV_TABLE_DECLARE_TABLE = $s601c_v,NV_CLARO_COLOMBIA_QTLCALLRECORDS_OP_TOMASG_20160324_180708452
--NV_TABLE_DECLARE_TABLE = $s602c_v,NV_CLARO_COLOMBIA_QTLCALLRECORDS_OP_TOMASG_20160414_155627684
--NV_TABLE_DECLARE_TABLE = $s603c_v,NV_CLARO_COLOMBIA_QTLCALLRECORDS_OP_DSALEK_20160516_125550384
--ETB:
--NV_TABLE_DECLARE_TABLE = $s511e,NV_COLOMBIA_MOVILE_SA_ESP_QTLBASICRAW_ROBERTT_20160107_150639357
--NV_TABLE_DECLARE_TABLE = $s512e,NV_COLOMBIA_MOVILE_SA_ESP_QTLBASICRAW_TOMASG_20160210_120047842
--NV_TABLE_DECLARE_TABLE = $s601e,NV_ETB_QTLCALLRECORDS_OP_DSALEK_20160408_145007098
--NV_TABLE_DECLARE_TABLE = $s602e,NV_ETB_QTLCALLRECORDS_OP_DSALEK_20160422_094202927
--NV_TABLE_DECLARE_TABLE = $s603e,NV_ETB_QTLCALLRECORDS_OP_DSALEK_20160512_221123656
--Telefonica:
--NV_TABLE_DECLARE_TABLE = $s511f,NV_COLOMBIA_MOVILE_SA_ESP_QTLBASICRAW_ROBERTT_20160107_154934821
--NV_TABLE_DECLARE_TABLE = $s512f,NV_COLOMBIA_MOVILE_SA_ESP_QTLBASICRAW_TOMASG_20160209_151109456
--NV_TABLE_DECLARE_TABLE = $s601f_d,NV_TELEFONICA_COLOMBIA_QTLCALLRECORDS_OP_DSALEK_20160327_224648894
--NV_TABLE_DECLARE_TABLE = $s602f_d,NV_TELEFONICA_COLOMBIA_QTLCALLRECORDS_OP_DSALEK_20160415_073429733
--NV_TABLE_DECLARE_TABLE = $s603f_d,NV_TELEFONICA_COLOMBIA_QTLCALLRECORDS_OP_DSALEK_20160513_123731592
--NV_TABLE_DECLARE_TABLE = $s601f_v,NV_TELEFONICA_COLOMBIA_QTLCALLRECORDS_OP_DSALEK_20160328_144516166
--NV_TABLE_DECLARE_TABLE = $s602f_v,NV_TELEFONICA_COLOMBIA_QTLCALLRECORDS_OP_DSALEK_20160414_213710307
--NV_TABLE_DECLARE_TABLE = $s603f_v,NV_TELEFONICA_COLOMBIA_QTLCALLRECORDS_OP_DSALEK_20160513_081406011
--Tigo:
--NV_TABLE_DECLARE_TABLE = $s511g,NV_COLOMBIA_MOVILE_SA_ESP_QTLBASICRAW_ROBERTT_20160107_164211882
--NV_TABLE_DECLARE_TABLE = $s512g,NV_COLOMBIA_MOVILE_SA_ESP_QTLBASICRAW_TOMASG_20160212_223601473
--NV_TABLE_DECLARE_TABLE = $s601g,NV_TIGO_MILLICOM_COLOMBIA_QTLCALLRECORDS_OP_TOMASG_20160328_092803025
--NV_TABLE_DECLARE_TABLE = $s602g,NV_TIGO_MILLICOM_COLOMBIA_QTLCALLRECORDS_OP_DSALEK_20160417_081005115
--NV_TABLE_DECLARE_TABLE = $s603g,NV_TIGO_MILLICOM_COLOMBIA_QTLCALLRECORDS_OP_DSALEK_20160515_001119813
--NV_TABLE_DECLARE_END


--Config Variables
--NV_VARIABLE_DECLARE_BEGIN
--NVN variable syntax doesn't support commas, so add multiple options for plmnid separately. Must be same length as each other and new plmnid (unless want to change imsi length)
--Use a dummy value if there is only 1 plmnid belonging to the home operator in source data:
--NV_VARIABLE_DECLARE_VARIABLE = $existing_plmnid_avantel,string,'732130'
--NV_VARIABLE_DECLARE_VARIABLE = $existing_plmnid1_claro,string,'0732101'
--NV_VARIABLE_DECLARE_VARIABLE = $existing_plmnid2_claro,string,'!!!!!!!'
--NV_VARIABLE_DECLARE_VARIABLE = $existing_plmnid_etb,string,'732187'
--NV_VARIABLE_DECLARE_VARIABLE = $existing_plmnid1_telefonica,string,'732123'
--NV_VARIABLE_DECLARE_VARIABLE = $existing_plmnid2_telefonica,string,'!!!!!!'
--NV_VARIABLE_DECLARE_VARIABLE = $existing_plmnid1_tigo,string,'732103'
--NV_VARIABLE_DECLARE_VARIABLE = $existing_plmnid2_tigo,string,'732111'
--NV_VARIABLE_DECLARE_VARIABLE = $new_plmnid,string,'410010'
--NV_VARIABLE_DECLARE_VARIABLE = $msisdn_pre,string,'92'
--Limiting variables:
--  For testing, set to (respectively):
--  > 10
--  > all
--  > --
--  > [just leave blank]
--  For complete output, set to:
--  > all
--  > [just leave blank]
--  > [just leave blank]
--  > --
--NV_VARIABLE_DECLARE_VARIABLE = $mylimit,string,10
--NV_VARIABLE_DECLARE_VARIABLE = $myunion,string,all
--NV_VARIABLE_DECLARE_VARIABLE = $kill,string,--
--NV_VARIABLE_DECLARE_VARIABLE = $keep,string,
--NV_VARIABLE_DECLARE_END
-- Copyright (c) 2018 Qualcomm Technologies, Inc.
--
--  All rights reserved.
--
--
--
--  Redistribution and use in source and binary forms, with or without modification, are permitted (subject to the
--  limitations in the disclaimer below) provided that the following conditions are met:
--
--
--  * Redistributions of source code must retain the above copyright notice, this list of conditions and the following
--  disclaimer.
--
--  * Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following
--  disclaimer in the documentation and/or other materials provided with the distribution.
--
--  * Neither the name of Qualcomm Technologies, Inc. nor the names of its contributors may be used to endorse or promote
--  products derived from this software without specific prior written permission.
--
--  NO EXPRESS OR IMPLIED LICENSES TO ANY PARTY'S PATENT RIGHTS ARE GRANTED BY THIS LICENSE. THIS SOFTWARE IS PROVIDED BY
--  THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
--  THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
--  COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
--  DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
--  OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR
--  TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
--  POSSIBILITY OF SUCH DAMAGE.
--

--log outputs table
create temp table loggy (
myindex int
,descr varchar(80)
,num bigint
,mycomment varchar(100)
)distribute on random;

--skip log of total records
--[deleted statements compared to individual scripts]


--do export
--try to preserve some interest in the data by only rewriting MSISDN/IMSI prefix if matches the original home operator



--Nov data generally has date_logged set to a single date, so use fetch_date_time instead

create temp table op1 as
(
--avantel:
(select
	substr(fetch_date_time,1,8) date
	,trim(imei) imei
	,case
		when substr(trim(imsi),1,length($existing_plmnid_avantel)) in ($existing_plmnid_avantel) then $new_plmnid || substr(trim(imsi),length($existing_plmnid_avantel)+1) 
		else trim(imsi)
	end imsi
	,case
		when substr(trim(imsi),1,length($existing_plmnid_avantel)) in ($existing_plmnid_avantel) then $msisdn_pre || substr(trim(imsi), length($msisdn_pre)+1)
		else trim(imsi)
	end msisdn
	$keep ,'avantel' opname
from $s511a
$kill where date between '20151101' and '20151130' and trim(imsi) is not null
limit $mylimit)
union $myunion
--claro data:
(select 
	substr(fetch_date_time,1,8) date
	,imei
	,case
		when substr(imsi,1,length($existing_plmnid1_claro)) in ($existing_plmnid1_claro,$existing_plmnid2_claro) then $new_plmnid || substr(imsi,length($existing_plmnid1_claro)+1) 
		else substr(IMSI,2)
	end imsi
	,case
		when substr(imsi,1,length($existing_plmnid1_claro)) in ($existing_plmnid1_claro,$existing_plmnid2_claro) then $msisdn_pre || substr(imsi, length($msisdn_pre)+2)
		else substr(IMSI,2)
	end msisdn
	$keep ,'Claro data' opname
from $s511c_d
$kill where date between '20151101' and '20151130' and imsi <> 'imsi'
limit $mylimit)
union $myunion
--claro voice:
(select
	substr(fetch_date_time,1,8) date
	,imei
	,case
		when substr(imsi,1,length($existing_plmnid1_claro)) in ($existing_plmnid1_claro,$existing_plmnid2_claro) then $new_plmnid || substr(imsi,length($existing_plmnid1_claro)+1) 
		else substr(IMSI,2)
	end imsi
	,case
		when substr(imsi,1,length($existing_plmnid1_claro)) in ($existing_plmnid1_claro,$existing_plmnid2_claro) then $msisdn_pre || substr(imsi, length($msisdn_pre)+2)
		else substr(IMSI,2)
	end msisdn
	$keep ,'Claro voice' opname
from $s511c_v
$kill where date between '20151101' and '20151130'
limit $mylimit)
union $myunion
--etb:
(select 
	to_char(to_date(fetch_date_time, 'YYYY-MM-DD HH:MI:SS'),'YYYYMMDD') date
	,imei
	,case
		when substr(imsi,1,length($existing_plmnid_etb)) = $existing_plmnid_etb then $new_plmnid || substr(imsi,length($existing_plmnid_etb)+1,length(imsi)-length($existing_plmnid_etb)) 
		else IMSI
	end imsi
	,case
		when substr(imsi,1,length($existing_plmnid_etb)) = $existing_plmnid_etb  then $msisdn_pre || substr(imsi, length($msisdn_pre)+1, length(imsi)-length($msisdn_pre))
		else imsi
	end msisdn
	$keep ,'etb' opname
from $s511e
limit $mylimit)
union $myunion
--telefonica combined:
(select 
	substr(fetch_date_time,1,8) date
	,trim(imei) imei
	,case
		when substr(imsi,1,length($existing_plmnid1_telefonica)) in ($existing_plmnid1_telefonica,$existing_plmnid2_telefonica) then $new_plmnid || substr(rtrim(imsi),length($existing_plmnid1_telefonica)+1) 
		else imsi
	end imsi
	,case
		when substr(imsi,1,length($existing_plmnid1_telefonica)) in ($existing_plmnid1_telefonica,$existing_plmnid2_telefonica) then $msisdn_pre || substr(rtrim(imsi), length($msisdn_pre)+1)
		else imsi
	end msisdn
	$keep ,'telefonica' opname
from $s511f
$kill where date between '20151101' and '20151130'
limit $mylimit)
union $myunion
--tigo:
(select 
	to_char(to_date(fetch_date_time, 'DD/MM/YYYY HH:MI:SS'),'YYYYMMDD') date
	,imei
	,case
		when substr(imsi,1,length($existing_plmnid1_tigo)) in ($existing_plmnid1_tigo,$existing_plmnid2_tigo) then $new_plmnid || substr(imsi,length($existing_plmnid1_tigo)+1,length(imsi)-length($existing_plmnid1_tigo)) 
		else IMSI
	end imsi
	,case
		when substr(imsi,1,length($existing_plmnid1_tigo)) in ($existing_plmnid1_tigo,$existing_plmnid2_tigo) then $msisdn_pre || substr(imsi, length($msisdn_pre)+1, length(imsi)-length($msisdn_pre))
		else imsi
	end msisdn
	$keep ,'tigo' opname
from $s511g
$kill where date between '20151101' and '20151130'
limit $mylimit)
) distribute on random;


--dec data has date_logged as a date, not a varchar
create temp table op2 as
(
--avantel:
(select
	to_char(date_logged, 'YYYYMMDD') date
	,trim(imei) imei
	,case
		when substr(trim(imsi),1,length($existing_plmnid_avantel)) in ($existing_plmnid_avantel) then $new_plmnid || substr(trim(imsi),length($existing_plmnid_avantel)+1) 
		else trim(imsi)
	end imsi
	,case
		when substr(trim(imsi),1,length($existing_plmnid_avantel)) in ($existing_plmnid_avantel) then $msisdn_pre || substr(trim(imsi), length($msisdn_pre)+1)
		else trim(imsi)
	end msisdn
	$keep ,'avantel' opname
from $s512a
$kill where date between '20151201' and '20151231' and trim(imsi) is not null
limit $mylimit)
union $myunion
--claro data:
(select 
	to_char(date_logged, 'YYYYMMDD') date
	,imei
	,case
		when substr(imsi,1,length($existing_plmnid1_claro)) in ($existing_plmnid1_claro,$existing_plmnid2_claro) then $new_plmnid || substr(imsi,length($existing_plmnid1_claro)+1) 
		else substr(IMSI,2)
	end imsi
	,case
		when substr(imsi,1,length($existing_plmnid1_claro)) in ($existing_plmnid1_claro,$existing_plmnid2_claro) then $msisdn_pre || substr(imsi, length($msisdn_pre)+2)
		else substr(IMSI,2)
	end msisdn
	$keep ,'Claro data' opname
from $s512c_d
$kill where date between '20151201' and '20151231'
limit $mylimit)
union $myunion
--claro voice:
(select 
	to_char(date_logged, 'YYYYMMDD') date
	,imei
	,case
		when substr(imsi,1,length($existing_plmnid1_claro)) in ($existing_plmnid1_claro,$existing_plmnid2_claro) then $new_plmnid || substr(imsi,length($existing_plmnid1_claro)+1) 
		else substr(IMSI,2)
	end imsi
	,case
		when substr(imsi,1,length($existing_plmnid1_claro)) in ($existing_plmnid1_claro,$existing_plmnid2_claro) then $msisdn_pre || substr(imsi, length($msisdn_pre)+2)
		else substr(IMSI,2)
	end msisdn
	$keep ,'Claro voice' opname
from $s512c_v
$kill where date between '20151201' and '20151231'
limit $mylimit)
union $myunion
--ETB:
(select 
	to_char(date_logged, 'YYYYMMDD') date
	,imei
	,case
		when substr(imsi,1,length($existing_plmnid_etb)) = $existing_plmnid_etb then $new_plmnid || substr(imsi,length($existing_plmnid_etb)+1,length(imsi)-length($existing_plmnid_etb)) 
		else IMSI
	end imsi
	,case
		when substr(imsi,1,length($existing_plmnid_etb)) = $existing_plmnid_etb  then $msisdn_pre || substr(imsi, length($msisdn_pre)+1, length(imsi)-length($msisdn_pre))
		else imsi
	end msisdn
	$keep ,'etb' opname
from $s512e
limit $mylimit)
union $myunion
--telefonica combined:
(select 
	to_char(date_logged, 'YYYYMMDD') date
	,imei
	,case
		when substr(imsi,1,length($existing_plmnid1_telefonica)) in ($existing_plmnid1_telefonica,$existing_plmnid2_telefonica) then $new_plmnid || substr(imsi,length($existing_plmnid1_telefonica)+1) 
		else imsi
	end imsi
	,case
		when substr(imsi,1,length($existing_plmnid1_telefonica)) in ($existing_plmnid1_telefonica,$existing_plmnid2_telefonica) then $msisdn_pre || substr(imsi, length($msisdn_pre)+1)
		else imsi
	end msisdn
	$keep ,'telefonica' opname
from $s512f
$kill where date between '20151201' and '20151231'
limit $mylimit)
union $myunion
--tigo:
(select 
	to_char(date_logged, 'YYYYMMDD') date
	,imei
	,case
		when substr(imsi,1,length($existing_plmnid1_tigo)) in ($existing_plmnid1_tigo,$existing_plmnid2_tigo) then $new_plmnid || substr(imsi,length($existing_plmnid1_tigo)+1,length(imsi)-length($existing_plmnid1_tigo)) 
		else IMSI
	end imsi
	,case
		when substr(imsi,1,length($existing_plmnid1_tigo)) in ($existing_plmnid1_tigo,$existing_plmnid2_tigo) then $msisdn_pre || substr(imsi, length($msisdn_pre)+1, length(imsi)-length($msisdn_pre))
		else imsi
	end msisdn
	$keep ,'tigo' opname
from $s512g
$kill where date between '20151201' and '20151231'
limit $mylimit)
) distribute on random;


create temp table op3 as
(
--avantel:
(select
	substr(date_logged,1,8) date
	,trim(imei) imei
	,case
		when substr(trim(imsi),1,length($existing_plmnid_avantel)) in ($existing_plmnid_avantel) then $new_plmnid || substr(trim(imsi),length($existing_plmnid_avantel)+1) 
		else trim(imsi)
	end imsi
	,case
		when substr(trim(imsi),1,length($existing_plmnid_avantel)) in ($existing_plmnid_avantel) then $msisdn_pre || substr(trim(imsi), length($msisdn_pre)+1)
		else trim(imsi)
	end msisdn
	$keep ,'avantel' opname
from $s601a
$kill where date between '20160101' and '20160131' and trim(imsi) is not null
limit $mylimit)
union $myunion
--claro data:
(select 
	substr(date_logged,1,8) date
	,imei
	,case
		when substr(imsi,1,length($existing_plmnid1_claro)) in ($existing_plmnid1_claro,$existing_plmnid2_claro) then $new_plmnid || substr(imsi,length($existing_plmnid1_claro)+1) 
		else substr(IMSI,2)
	end imsi
	,case
		when substr(imsi,1,length($existing_plmnid1_claro)) in ($existing_plmnid1_claro,$existing_plmnid2_claro) then $msisdn_pre || substr(imsi, length($msisdn_pre)+2)
		else substr(IMSI,2)
	end msisdn
	$keep ,'Claro data' opname		
from $s601c_d
$kill where date between '20160101' and '20160131'
limit $mylimit)
union $myunion
--claro voice:
(select 
	date_logged date
	,imei
	,case
		when substr(imsi,1,length($existing_plmnid1_claro)) in ($existing_plmnid1_claro,$existing_plmnid2_claro) then $new_plmnid || substr(imsi,length($existing_plmnid1_claro)+1) 
		else substr(IMSI,2)
	end imsi
	,case
		when substr(imsi,1,length($existing_plmnid1_claro)) in ($existing_plmnid1_claro,$existing_plmnid2_claro) then $msisdn_pre || substr(imsi, length($msisdn_pre)+2)
		else substr(IMSI,2)
	end msisdn
	$keep ,'Claro voice' opname
from $s601c_v
$kill where date between '20160101' and '20160131'
limit $mylimit)
union $myunion
--ETB:
(select 
	date_logged date
	,imei
	,case
		when substr(imsi,1,length($existing_plmnid_etb)) = $existing_plmnid_etb then $new_plmnid || substr(imsi,length($existing_plmnid_etb)+1,length(imsi)-length($existing_plmnid_etb)) 
		else IMSI
	end imsi
	,case
		when substr(imsi,1,length($existing_plmnid_etb)) = $existing_plmnid_etb  then $msisdn_pre || substr(imsi, length($msisdn_pre)+1, length(imsi)-length($msisdn_pre))
		else imsi
	end msisdn
	$keep ,'etb' opname
from $s601e
$kill where date_logged is not null
limit $mylimit)
union $myunion
--telefonica data:
(select 
	substr(date_logged,1,8) date
	,imei
	,case
		when substr(imsi,1,length($existing_plmnid1_telefonica)) in ($existing_plmnid1_telefonica,$existing_plmnid2_telefonica) then $new_plmnid || substr(imsi,length($existing_plmnid1_telefonica)+1) 
		else imsi
	end imsi
	,case
		when substr(imsi,1,length($existing_plmnid1_telefonica)) in ($existing_plmnid1_telefonica,$existing_plmnid2_telefonica) then $msisdn_pre || substr(imsi, length($msisdn_pre)+1)
		else imsi
	end msisdn
	$keep ,'telefonica data' opname
from $s601f_d
$kill where date between '20160101' and '20160131'
limit $mylimit)
union $myunion
--telefonica voice:
(select 
	date_logged date
	,imei
	,case
		when substr(imsi,1,length($existing_plmnid1_telefonica)) in ($existing_plmnid1_telefonica,$existing_plmnid2_telefonica) then $new_plmnid || substr(imsi,length($existing_plmnid1_telefonica)+1) 
		else imsi
	end imsi
	,case
		when substr(imsi,1,length($existing_plmnid1_telefonica)) in ($existing_plmnid1_telefonica,$existing_plmnid2_telefonica) then $msisdn_pre || substr(imsi, length($msisdn_pre)+1)
		else imsi
	end msisdn
	$keep ,'telefonica voice' opname
from $s601f_v
$kill where date between '20160101' and '20160131'
limit $mylimit)
union $myunion
--tigo:
(select 
	date_logged date
	,imei
	,case
		when substr(imsi,1,length($existing_plmnid1_tigo)) in ($existing_plmnid1_tigo,$existing_plmnid2_tigo) then $new_plmnid || substr(imsi,length($existing_plmnid1_tigo)+1,length(imsi)-length($existing_plmnid1_tigo)) 
		else IMSI
	end imsi
	,case
		when substr(imsi,1,length($existing_plmnid1_tigo)) in ($existing_plmnid1_tigo,$existing_plmnid2_tigo) then $msisdn_pre || substr(imsi, length($msisdn_pre)+1, length(imsi)-length($msisdn_pre))
		else imsi
	end msisdn
	$keep ,'tigo' opname
from $s601g
$kill where date between '20160101' and '20160131'
limit $mylimit)
) distribute on random;


create temp table op4 as
(
--avantel:
--*********[no avantel data for feb]************
--claro data:
(select 
	date_logged date
	,imei
	,case
		when substr(imsi,1,length($existing_plmnid1_claro)) in ($existing_plmnid1_claro,$existing_plmnid2_claro) then $new_plmnid || substr(imsi,length($existing_plmnid1_claro)+1) 
		else substr(IMSI,2)
	end imsi
	,case
		when substr(imsi,1,length($existing_plmnid1_claro)) in ($existing_plmnid1_claro,$existing_plmnid2_claro) then $msisdn_pre || substr(imsi, length($msisdn_pre)+2)
		else substr(IMSI,2)
	end msisdn
	$keep ,'Claro data' opname
from $s602c_d
$kill where date between '20160201' and '20160229'
limit $mylimit)
union $myunion
--claro voice:
(select 
	date_logged date
	,imei
	,case
		when substr(imsi,1,length($existing_plmnid1_claro)) in ($existing_plmnid1_claro,$existing_plmnid2_claro) then $new_plmnid || substr(imsi,length($existing_plmnid1_claro)+1) 
		else substr(IMSI,2)
	end imsi
	,case
		when substr(imsi,1,length($existing_plmnid1_claro)) in ($existing_plmnid1_claro,$existing_plmnid2_claro) then $msisdn_pre || substr(imsi, length($msisdn_pre)+2)
		else substr(IMSI,2)
	end msisdn
	$keep ,'Claro voice' opname
from $s602c_v
$kill where date between '20160201' and '20160229'
limit $mylimit)
union $myunion
--ETB:
(select 
	date_logged date
	,imei
	,case
		when substr(imsi,1,length($existing_plmnid_etb)) = $existing_plmnid_etb then $new_plmnid || substr(imsi,length($existing_plmnid_etb)+1,length(imsi)-length($existing_plmnid_etb)) 
		else IMSI
	end imsi
	,case
		when substr(imsi,1,length($existing_plmnid_etb)) = $existing_plmnid_etb  then $msisdn_pre || substr(imsi, length($msisdn_pre)+1, length(imsi)-length($msisdn_pre))
		else imsi
	end msisdn
	$keep ,'etb' opname
from $s602e
$kill where date_logged is not null
limit $mylimit)
union $myunion
--telefonica data:
(select 
	date_logged date
	,imei
	,case
		when substr(imsi,1,length($existing_plmnid1_telefonica)) in ($existing_plmnid1_telefonica,$existing_plmnid2_telefonica) then $new_plmnid || substr(imsi,length($existing_plmnid1_telefonica)+1) 
		else imsi
	end imsi
	,case
		when substr(imsi,1,length($existing_plmnid1_telefonica)) in ($existing_plmnid1_telefonica,$existing_plmnid2_telefonica) then $msisdn_pre || substr(imsi, length($msisdn_pre)+1)
		else imsi
	end msisdn
	$keep ,'telefonica data' opname
from $s602f_d
$kill where date between '20160201' and '20160229'
limit $mylimit)
union $myunion
--telefonica voice:
(select 
	date_logged date
	,imei
	,case
		when substr(imsi,1,length($existing_plmnid1_telefonica)) in ($existing_plmnid1_telefonica,$existing_plmnid2_telefonica) then $new_plmnid || substr(imsi,length($existing_plmnid1_telefonica)+1) 
		else imsi
	end imsi
	,case
		when substr(imsi,1,length($existing_plmnid1_telefonica)) in ($existing_plmnid1_telefonica,$existing_plmnid2_telefonica) then $msisdn_pre || substr(imsi, length($msisdn_pre)+1)
		else imsi
	end msisdn
	$keep ,'telefonica voice' opname
from $s602f_v
$kill where date between '20160201' and '20160229'
limit $mylimit)
union $myunion
--tigo:
(select 
	date_logged date
	,imei
	,case
		when substr(imsi,1,length($existing_plmnid1_tigo)) in ($existing_plmnid1_tigo,$existing_plmnid2_tigo) then $new_plmnid || substr(imsi,length($existing_plmnid1_tigo)+1,length(imsi)-length($existing_plmnid1_tigo)) 
		else IMSI
	end imsi
	,case
		when substr(imsi,1,length($existing_plmnid1_tigo)) in ($existing_plmnid1_tigo,$existing_plmnid2_tigo) then $msisdn_pre || substr(imsi, length($msisdn_pre)+1, length(imsi)-length($msisdn_pre))
		else imsi
	end msisdn
	$keep ,'tigo' opname
from $s602g
$kill where date between '20160201' and '20160229'
limit $mylimit)
) distribute on random;

create temp table op5 as
(
--avantel:
(select
	date_logged date
	,trim(imei) imei
	,case
		when substr(trim(imsi),1,length($existing_plmnid_avantel)) in ($existing_plmnid_avantel) then $new_plmnid || substr(trim(imsi),length($existing_plmnid_avantel)+1) 
		else trim(imsi)
	end imsi
	,case
		when substr(trim(imsi),1,length($existing_plmnid_avantel)) in ($existing_plmnid_avantel) then $msisdn_pre || substr(trim(imsi), length($msisdn_pre)+1)
		else trim(imsi)
	end msisdn
	$keep ,'avantel' opname
from $s603a
$kill where date between '20160301' and '20160331' and trim(imsi) is not null
limit $mylimit)
union $myunion
--claro data:
(select 
	date_logged date
	,imei
	,case
		when substr(imsi,1,length($existing_plmnid1_claro)) in ($existing_plmnid1_claro,$existing_plmnid2_claro) then $new_plmnid || substr(imsi,length($existing_plmnid1_claro)+1) 
		else substr(IMSI,2)
	end imsi
	,case
		when substr(imsi,1,length($existing_plmnid1_claro)) in ($existing_plmnid1_claro,$existing_plmnid2_claro) then $msisdn_pre || substr(imsi, length($msisdn_pre)+2)
		else substr(IMSI,2)
	end msisdn
	$keep ,'Claro data' opname
from $s603c_d
$kill where date between '20160301' and '20160331'
limit $mylimit)
union $myunion
--claro voice:
(select 
	date_logged date
	,imei
	,case
		when substr(imsi,1,length($existing_plmnid1_claro)) in ($existing_plmnid1_claro,$existing_plmnid2_claro) then $new_plmnid || substr(imsi,length($existing_plmnid1_claro)+1) 
		else substr(IMSI,2)
	end imsi
	,case
		when substr(imsi,1,length($existing_plmnid1_claro)) in ($existing_plmnid1_claro,$existing_plmnid2_claro) then $msisdn_pre || substr(imsi, length($msisdn_pre)+2)
		else substr(IMSI,2)
	end msisdn
	$keep ,'Claro voice' opname
from $s603c_v
$kill where date between '20160301' and '20160331'
limit $mylimit)
union $myunion
--ETB:
(select 
	date_logged date
	,imei
	,case
		when substr(imsi,1,length($existing_plmnid_etb)) = $existing_plmnid_etb then $new_plmnid || substr(imsi,length($existing_plmnid_etb)+1,length(imsi)-length($existing_plmnid_etb)) 
		else IMSI
	end imsi
	,case
		when substr(imsi,1,length($existing_plmnid_etb)) = $existing_plmnid_etb  then $msisdn_pre || substr(imsi, length($msisdn_pre)+1, length(imsi)-length($msisdn_pre))
		else imsi
	end msisdn
	$keep ,'etb' opname
from $s603e
$kill where date_logged is not null
limit $mylimit)
union $myunion
--telefonica data:
(select 
	date_logged date
	,imei
	,case
		when substr(imsi,1,length($existing_plmnid1_telefonica)) in ($existing_plmnid1_telefonica,$existing_plmnid2_telefonica) then $new_plmnid || substr(imsi,length($existing_plmnid1_telefonica)+1) 
		else imsi
	end imsi
	,case
		when substr(imsi,1,length($existing_plmnid1_telefonica)) in ($existing_plmnid1_telefonica,$existing_plmnid2_telefonica) then $msisdn_pre || substr(imsi, length($msisdn_pre)+1)
		else imsi
	end msisdn
	$keep ,'telefonica data' opname
from $s603f_d
$kill where date between '20160301' and '20160331'
limit $mylimit)
union $myunion
--telefonica voice:
(select 
	date_logged date
	,imei
	,case
		when substr(imsi,1,length($existing_plmnid1_telefonica)) in ($existing_plmnid1_telefonica,$existing_plmnid2_telefonica) then $new_plmnid || substr(imsi,length($existing_plmnid1_telefonica)+1) 
		else imsi
	end imsi
	,case
		when substr(imsi,1,length($existing_plmnid1_telefonica)) in ($existing_plmnid1_telefonica,$existing_plmnid2_telefonica) then $msisdn_pre || substr(imsi, length($msisdn_pre)+1)
		else imsi
	end msisdn
	$keep ,'telefonica voice' opname
from $s603f_v
$kill where date between '20160301' and '20160331'
limit $mylimit)
union $myunion
--tigo:
(select 
	date_logged date
	,imei
	,case
		when substr(imsi,1,length($existing_plmnid1_tigo)) in ($existing_plmnid1_tigo,$existing_plmnid2_tigo) then $new_plmnid || substr(imsi,length($existing_plmnid1_tigo)+1,length(imsi)-length($existing_plmnid1_tigo)) 
		else IMSI
	end imsi
	,case
		when substr(imsi,1,length($existing_plmnid1_tigo)) in ($existing_plmnid1_tigo,$existing_plmnid2_tigo) then $msisdn_pre || substr(imsi, length($msisdn_pre)+1, length(imsi)-length($msisdn_pre))
		else imsi
	end msisdn
	$keep ,'tigo' opname
from $s603g
$kill where date between '20160301' and '20160331'
limit $mylimit)
) distribute on random;

insert into loggy (myindex, descr, num, mycomment) select 1100, 'distinct count 2015-11', count(*), NULL from op1;
insert into loggy (myindex, descr, num, mycomment) select 1200, 'distinct count 2015-12', count(*), NULL from op2;
insert into loggy (myindex, descr, num, mycomment) select 1300, 'distinct count 2016-01', count(*), NULL from op3;
insert into loggy (myindex, descr, num, mycomment) select 1400, 'distinct count 2016-02', count(*), NULL from op4;
insert into loggy (myindex, descr, num, mycomment) select 1500, 'distinct count 2016-03', count(*), NULL from op5;

--compute null counts, to compare with DIRBS import checks
--insert into loggy (myindex, descr, num, mycomment) 
--select 
--	1600
--	,'count any null in triplet 2015-11'
--	,count(*)
--	,NULL
--from 
--	op1
--where 
--	imsi = '' or imsi is null
--	or imei = '' or imei is null
--	or msisdn = '' or msisdn is null
--;
--
--insert into loggy (myindex, descr, num, mycomment) 
--select 
--	1700
--	,'count any null in triplet 2015-12'
--	,count(*)
--	,NULL
--from 
--	op2
--where 
--	imsi = '' or imsi is null
--	or imei = '' or imei is null
--	or msisdn = '' or msisdn is null
--;
--insert into loggy (myindex, descr, num, mycomment) 
--select 
--	1800
--	,'count any null in triplet 2016-01'
--	,count(*)
--	,NULL
--from 
--	op3
--where 
--	imsi = '' or imsi is null
--	or imei = '' or imei is null
--	or msisdn = '' or msisdn is null
--;
--insert into loggy (myindex, descr, num, mycomment) 
--select 
--	1900
--	,'count any null in triplet 2016-02'
--	,count(*)
--	,NULL
--from 
--	op4
--where 
--	imsi = '' or imsi is null
--	or imei = '' or imei is null
--	or msisdn = '' or msisdn is null
--;
--insert into loggy (myindex, descr, num, mycomment) 
--select 
--	2000
--	,'count any null in triplet 2016-03'
--	,count(*)
--	,NULL
--from 
--	op5
--where 
--	imsi = '' or imsi is null
--	or imei = '' or imei is null
--	or msisdn = '' or msisdn is null
--;

select myindex, descr,num, mycomment from loggy order by myindex;

--filter the output in case any stray characters made it through
select * from op1 where regexp_like(imei, '^[0-9A-Fa-f*#]+$') and regexp_like(imsi, '^[0-9]+$');
select * from op2 where regexp_like(imei, '^[0-9A-Fa-f*#]+$') and regexp_like(imsi, '^[0-9]+$');
select * from op3 where regexp_like(imei, '^[0-9A-Fa-f*#]+$') and regexp_like(imsi, '^[0-9]+$');
select * from op4 where regexp_like(imei, '^[0-9A-Fa-f*#]+$') and regexp_like(imsi, '^[0-9]+$');
select * from op5 where regexp_like(imei, '^[0-9A-Fa-f*#]+$') and regexp_like(imsi, '^[0-9]+$');
