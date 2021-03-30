--
-- PostgreSQL database dump
--

-- Dumped from database version 9.6.1
-- Dumped by pg_dump version 9.6.2
--
--- Copyright (c) 2018-2021 Qualcomm Technologies, Inc.
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
-- Started on 2017-05-16 16:10:36 AEST

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET row_security = off;
SET search_path = core, pg_catalog;

--
-- TOC entry 599 (class 1247 OID 143600)
-- Name: has_status_type; Type: TYPE; Schema: core; Owner: -
--

CREATE TYPE has_status_type AS ENUM (
    'Not Known',
    'Y',
    'N'
);


--
-- TOC entry 207 (class 1255 OID 143733)
-- Name: fail_on_seen_imeis_table_insert(); Type: FUNCTION; Schema: core; Owner: -
--

CREATE FUNCTION fail_on_seen_imeis_table_insert() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    RAISE EXCEPTION 'Trying to insert directly into seen_imeis table! Insert into child table directly instead.';
    RETURN NULL;
END
$$;


--
-- TOC entry 208 (class 1255 OID 143741)
-- Name: fail_on_seen_triplets_table_insert(); Type: FUNCTION; Schema: core; Owner: -
--

CREATE FUNCTION fail_on_seen_triplets_table_insert() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    RAISE EXCEPTION 'Trying to insert directly into seen_triplets table! Insert into child table directly instead.';
    RETURN NULL;
END
$$;


--
-- TOC entry 214 (class 1255 OID 143761)
-- Name: hash_triplet(text, text, text); Type: FUNCTION; Schema: core; Owner: -
--

CREATE FUNCTION hash_triplet(imei_norm text, imsi text, msisdn text) RETURNS uuid
    LANGUAGE sql IMMUTABLE PARALLEL SAFE
    AS $$
    SELECT MD5(imei_norm || '@' ||
               COALESCE(imsi, '') || '@' ||
               COALESCE(msisdn, ''))::UUID;
$$;


--
-- TOC entry 212 (class 1255 OID 143759)
-- Name: normalize_imei(text); Type: FUNCTION; Schema: core; Owner: -
--

CREATE FUNCTION normalize_imei(imei text) RETURNS text
    LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE
    AS $$
    SELECT CASE WHEN imei ~ '^\d{14}' THEN LEFT(imei, 14) ELSE UPPER(imei) END;
$$;


--
-- TOC entry 211 (class 1255 OID 143758)
-- Name: schema_version_delete_functions(); Type: FUNCTION; Schema: core; Owner: -
--

CREATE FUNCTION schema_version_delete_functions() RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE
    func_oid oid;
BEGIN
    FOR func_oid IN SELECT p.oid FROM pg_proc p
    JOIN pg_namespace ON pg_namespace.oid = p.pronamespace
    JOIN pg_type ON pg_type.oid = p.prorettype
    WHERE pg_namespace.nspname = 'core'
    AND pg_type.typname != 'trigger'
    LOOP
        EXECUTE FORMAT('DROP FUNCTION %s(%s);',
                       func_oid::regproc,
                       pg_get_function_identity_arguments(func_oid));
    END LOOP;
    RETURN;
END
$$;


--
-- TOC entry 209 (class 1255 OID 143756)
-- Name: schema_version_get(); Type: FUNCTION; Schema: core; Owner: -
--

CREATE FUNCTION schema_version_get() RETURNS integer
    LANGUAGE plpgsql
    AS $$
DECLARE
    version INTEGER;
BEGIN
    SELECT MAX(sv.version) INTO STRICT version FROM schema_version sv;
    RETURN version;
END
$$;


--
-- TOC entry 210 (class 1255 OID 143757)
-- Name: schema_version_set(integer); Type: FUNCTION; Schema: core; Owner: -
--

CREATE FUNCTION schema_version_set(new_version integer) RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE
    num_rows INTEGER;
BEGIN
    SELECT count(*) INTO STRICT num_rows FROM schema_version;
    IF num_rows > 0
    THEN
        UPDATE schema_version SET version = new_version;
    ELSE
        INSERT INTO schema_version(version) VALUES(new_version);
    END IF;
END
$$;


--
-- TOC entry 213 (class 1255 OID 143760)
-- Name: starts_with_prefix(text, text[]); Type: FUNCTION; Schema: core; Owner: -
--

CREATE FUNCTION starts_with_prefix(str text, prefix_array text[]) RETURNS boolean
    LANGUAGE sql IMMUTABLE PARALLEL SAFE
    AS $$
    SELECT str LIKE ANY(prefix_array);
$$;


--
-- TOC entry 216 (class 1255 OID 143763)
-- Name: triplet_fails_clean_check(text, text, text, text); Type: FUNCTION; Schema: core; Owner: -
--

CREATE FUNCTION triplet_fails_clean_check(imei text, imei_norm text, imsi text, msisdn text) RETURNS boolean
    LANGUAGE sql IMMUTABLE PARALLEL SAFE
    AS $$
    SELECT msisdn IS NULL
           OR NOT COALESCE(LENGTH(imsi) BETWEEN 14 AND 15, FALSE)
           OR (UPPER(imei) = imei_norm AND imei !~ '^\d{14}');
$$;


--
-- TOC entry 215 (class 1255 OID 143762)
-- Name: triplet_fails_null_check(text, text, text); Type: FUNCTION; Schema: core; Owner: -
--

CREATE FUNCTION triplet_fails_null_check(imei_norm text, imsi text, msisdn text) RETURNS boolean
    LANGUAGE sql IMMUTABLE PARALLEL SAFE
    AS $$
    SELECT imei_norm is NULL OR imsi IS NULL OR msisdn IS NULL;
$$;


--
-- TOC entry 217 (class 1255 OID 143764)
-- Name: triplet_fails_region_check(text, text, text[], text[]); Type: FUNCTION; Schema: core; Owner: -
--

CREATE FUNCTION triplet_fails_region_check(imsi text, msisdn text, valid_mccs text[], valid_ccs text[]) RETURNS boolean
    LANGUAGE sql IMMUTABLE PARALLEL SAFE
    AS $$
    SELECT NOT starts_with_prefix(imsi, valid_mccs)
           OR NOT starts_with_prefix(msisdn, valid_ccs);
$$;


SET default_tablespace = '';

SET default_with_oids = false;

--
-- TOC entry 186 (class 1259 OID 143577)
-- Name: classification_metadata; Type: TABLE; Schema: core; Owner: -
--

CREATE TABLE classification_metadata (
    run_id integer NOT NULL,
    run_date timestamp with time zone NOT NULL,
    metadata jsonb NOT NULL
);


--
-- TOC entry 185 (class 1259 OID 143575)
-- Name: classification_run_id_seq; Type: SEQUENCE; Schema: core; Owner: -
--

CREATE SEQUENCE classification_run_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- TOC entry 188 (class 1259 OID 143587)
-- Name: classification_state; Type: TABLE; Schema: core; Owner: -
--

CREATE TABLE classification_state (
    run_id integer NOT NULL,
    start_date date NOT NULL,
    end_date date,
    block_date date NOT NULL,
    imei_norm text NOT NULL,
    cond_name text NOT NULL,
    row_id bigint NOT NULL
)
WITH (fillfactor='80');


--
-- TOC entry 187 (class 1259 OID 143585)
-- Name: classification_state_row_id_seq; Type: SEQUENCE; Schema: core; Owner: -
--

CREATE SEQUENCE classification_state_row_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- TOC entry 2272 (class 0 OID 0)
-- Dependencies: 187
-- Name: classification_state_row_id_seq; Type: SEQUENCE OWNED BY; Schema: core; Owner: -
--

ALTER SEQUENCE classification_state_row_id_seq OWNED BY classification_state.row_id;


--
-- TOC entry 189 (class 1259 OID 143607)
-- Name: gsma_data; Type: TABLE; Schema: core; Owner: -
--

CREATE TABLE gsma_data (
    tac character varying(8) NOT NULL,
    manufacturer character varying(128),
    bands character varying(4096),
    allocation_date date,
    model_name character varying(1024),
    optional_fields jsonb
);


--
-- TOC entry 190 (class 1259 OID 143615)
-- Name: import_id_seq; Type: SEQUENCE; Schema: core; Owner: -
--

CREATE SEQUENCE import_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- TOC entry 191 (class 1259 OID 143617)
-- Name: import_metadata; Type: TABLE; Schema: core; Owner: -
--

CREATE TABLE import_metadata (
    import_id integer NOT NULL,
    import_date timestamp with time zone NOT NULL,
    import_type text NOT NULL,
    metadata jsonb NOT NULL
);


--
-- TOC entry 204 (class 1259 OID 143735)
-- Name: seen_triplets; Type: TABLE; Schema: core; Owner: -
--

CREATE TABLE seen_triplets (
    triplet_year smallint NOT NULL,
    triplet_month smallint NOT NULL,
    first_seen date NOT NULL,
    last_seen date NOT NULL,
    date_bitmask integer NOT NULL,
    triplet_hash uuid NOT NULL,
    imei_norm text NOT NULL,
    imsi text,
    msisdn text,
    operator_id text NOT NULL,
    import_bitmasks bigint[] NOT NULL
);


--
-- TOC entry 206 (class 1259 OID 143751)
-- Name: operator_data; Type: VIEW; Schema: core; Owner: -
--

CREATE VIEW operator_data AS
 SELECT (sq.operator_id)::character varying(16) AS id,
    sq.import_id,
    sq.msisdn,
    sq.imei_norm AS imei,
    sq.imei_norm,
    sq.imsi,
    sq.connection_date
   FROM ( SELECT seen_triplets.operator_id,
            ((seen_triplets.import_bitmasks[1] >> 32))::integer AS import_id,
            make_date((seen_triplets.triplet_year)::integer, (seen_triplets.triplet_month)::integer, dom.dom) AS connection_date,
            seen_triplets.imei_norm,
            seen_triplets.imsi,
            seen_triplets.msisdn,
            seen_triplets.triplet_year,
            seen_triplets.triplet_month
           FROM generate_series(1, 31) dom(dom),
            seen_triplets
          WHERE ((seen_triplets.date_bitmask & (1 << (dom.dom - 1))) <> 0)) sq;


--
-- TOC entry 193 (class 1259 OID 143627)
-- Name: pairing_list; Type: TABLE; Schema: core; Owner: -
--

CREATE TABLE pairing_list (
    row_id bigint NOT NULL,
    imei text NOT NULL,
    imei_norm text NOT NULL,
    imsi text NOT NULL
);


--
-- TOC entry 192 (class 1259 OID 143625)
-- Name: pairing_list_row_id_seq; Type: SEQUENCE; Schema: core; Owner: -
--

CREATE SEQUENCE pairing_list_row_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- TOC entry 2273 (class 0 OID 0)
-- Dependencies: 192
-- Name: pairing_list_row_id_seq; Type: SEQUENCE OWNED BY; Schema: core; Owner: -
--

ALTER SEQUENCE pairing_list_row_id_seq OWNED BY pairing_list.row_id;


--
-- TOC entry 199 (class 1259 OID 143687)
-- Name: report_blacklist_violation_stats; Type: TABLE; Schema: core; Owner: -
--

CREATE TABLE report_blacklist_violation_stats (
    run_id integer NOT NULL,
    violation_age integer NOT NULL,
    num_imeis bigint NOT NULL,
    operator_id text NOT NULL
);


--
-- TOC entry 196 (class 1259 OID 143648)
-- Name: report_daily_stats; Type: TABLE; Schema: core; Owner: -
--

CREATE TABLE report_daily_stats (
    run_id integer NOT NULL,
    num_triplets bigint NOT NULL,
    num_imeis bigint NOT NULL,
    num_imsis bigint NOT NULL,
    num_msisdns bigint NOT NULL,
    data_date date NOT NULL,
    operator_id text NOT NULL
);


--
-- TOC entry 195 (class 1259 OID 143639)
-- Name: report_metadata; Type: TABLE; Schema: core; Owner: -
--

CREATE TABLE report_metadata (
    run_id integer NOT NULL,
    data_date date NOT NULL,
    report_year smallint NOT NULL,
    report_month smallint NOT NULL,
    metadata jsonb NOT NULL
);


--
-- TOC entry 194 (class 1259 OID 143637)
-- Name: report_metadata_run_id_seq; Type: SEQUENCE; Schema: core; Owner: -
--

CREATE SEQUENCE report_metadata_run_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- TOC entry 2274 (class 0 OID 0)
-- Dependencies: 194
-- Name: report_metadata_run_id_seq; Type: SEQUENCE OWNED BY; Schema: core; Owner: -
--

ALTER SEQUENCE report_metadata_run_id_seq OWNED BY report_metadata.run_id;


--
-- TOC entry 198 (class 1259 OID 143674)
-- Name: report_monthly_condition_stats; Type: TABLE; Schema: core; Owner: -
--

CREATE TABLE report_monthly_condition_stats (
    run_id integer NOT NULL,
    was_blocking boolean NOT NULL,
    num_imeis bigint NOT NULL,
    cond_name text NOT NULL,
    operator_id text NOT NULL
);


--
-- TOC entry 197 (class 1259 OID 143661)
-- Name: report_monthly_stats; Type: TABLE; Schema: core; Owner: -
--

CREATE TABLE report_monthly_stats (
    run_id integer NOT NULL,
    num_triplets bigint NOT NULL,
    num_imeis bigint NOT NULL,
    num_imsis bigint NOT NULL,
    num_msisdns bigint NOT NULL,
    num_gross_adds bigint NOT NULL,
    num_compliant_imeis bigint NOT NULL,
    num_noncompliant_imeis bigint NOT NULL,
    num_noncompliant_imeis_blocking bigint NOT NULL,
    num_noncompliant_imeis_info_only bigint NOT NULL,
    num_blacklist_add_imeis bigint NOT NULL,
    num_blacklist_add_imsis bigint NOT NULL,
    operator_id text NOT NULL
);


--
-- TOC entry 201 (class 1259 OID 143713)
-- Name: report_monthly_top_models_gross_adds; Type: TABLE; Schema: core; Owner: -
--

CREATE TABLE report_monthly_top_models_gross_adds (
    run_id integer NOT NULL,
    rank_pos smallint NOT NULL,
    num_imeis bigint NOT NULL,
    model text NOT NULL,
    manufacturer text NOT NULL,
    operator_id text NOT NULL
);


--
-- TOC entry 200 (class 1259 OID 143700)
-- Name: report_monthly_top_models_imei; Type: TABLE; Schema: core; Owner: -
--

CREATE TABLE report_monthly_top_models_imei (
    run_id integer NOT NULL,
    rank_pos smallint NOT NULL,
    num_imeis bigint NOT NULL,
    model text NOT NULL,
    manufacturer text NOT NULL,
    operator_id text NOT NULL
);


--
-- TOC entry 202 (class 1259 OID 143721)
-- Name: schema_version; Type: TABLE; Schema: core; Owner: -
--

CREATE TABLE schema_version (
    version integer DEFAULT 1 NOT NULL
);


--
-- TOC entry 203 (class 1259 OID 143727)
-- Name: seen_imeis; Type: TABLE; Schema: core; Owner: -
--

CREATE TABLE seen_imeis (
    first_seen_import_id integer NOT NULL,
    first_seen date NOT NULL,
    imei_norm text NOT NULL,
    operator_id text NOT NULL
);


--
-- TOC entry 205 (class 1259 OID 143743)
-- Name: stolen_list; Type: TABLE; Schema: core; Owner: -
--

CREATE TABLE stolen_list (
    imei text NOT NULL,
    imei_norm text NOT NULL
);


--
-- TOC entry 2104 (class 2604 OID 143590)
-- Name: classification_state row_id; Type: DEFAULT; Schema: core; Owner: -
--

ALTER TABLE ONLY classification_state ALTER COLUMN row_id SET DEFAULT nextval('classification_state_row_id_seq'::regclass);


--
-- TOC entry 2105 (class 2604 OID 143630)
-- Name: pairing_list row_id; Type: DEFAULT; Schema: core; Owner: -
--

ALTER TABLE ONLY pairing_list ALTER COLUMN row_id SET DEFAULT nextval('pairing_list_row_id_seq'::regclass);


--
-- TOC entry 2106 (class 2604 OID 143642)
-- Name: report_metadata run_id; Type: DEFAULT; Schema: core; Owner: -
--

ALTER TABLE ONLY report_metadata ALTER COLUMN run_id SET DEFAULT nextval('report_metadata_run_id_seq'::regclass);


--
-- TOC entry 2109 (class 2606 OID 143584)
-- Name: classification_metadata classification_metadata_pkey; Type: CONSTRAINT; Schema: core; Owner: -
--

ALTER TABLE ONLY classification_metadata
    ADD CONSTRAINT classification_metadata_pkey PRIMARY KEY (run_id);


--
-- TOC entry 2114 (class 2606 OID 143595)
-- Name: classification_state classification_state_pkey; Type: CONSTRAINT; Schema: core; Owner: -
--

ALTER TABLE ONLY classification_state
    ADD CONSTRAINT classification_state_pkey PRIMARY KEY (row_id);


--
-- TOC entry 2116 (class 2606 OID 143614)
-- Name: gsma_data gsma_data_pkey; Type: CONSTRAINT; Schema: core; Owner: -
--

ALTER TABLE ONLY gsma_data
    ADD CONSTRAINT gsma_data_pkey PRIMARY KEY (tac);


--
-- TOC entry 2118 (class 2606 OID 143624)
-- Name: import_metadata import_metadata_pkey; Type: CONSTRAINT; Schema: core; Owner: -
--

ALTER TABLE ONLY import_metadata
    ADD CONSTRAINT import_metadata_pkey PRIMARY KEY (import_id);


--
-- TOC entry 2121 (class 2606 OID 143635)
-- Name: pairing_list pairing_list_pkey; Type: CONSTRAINT; Schema: core; Owner: -
--

ALTER TABLE ONLY pairing_list
    ADD CONSTRAINT pairing_list_pkey PRIMARY KEY (row_id);


--
-- TOC entry 2131 (class 2606 OID 143694)
-- Name: report_blacklist_violation_stats report_blacklist_violation_stats_pkey; Type: CONSTRAINT; Schema: core; Owner: -
--

ALTER TABLE ONLY report_blacklist_violation_stats
    ADD CONSTRAINT report_blacklist_violation_stats_pkey PRIMARY KEY (run_id, operator_id, violation_age);


--
-- TOC entry 2125 (class 2606 OID 143655)
-- Name: report_daily_stats report_daily_stats_pkey; Type: CONSTRAINT; Schema: core; Owner: -
--

ALTER TABLE ONLY report_daily_stats
    ADD CONSTRAINT report_daily_stats_pkey PRIMARY KEY (run_id, operator_id, data_date);


--
-- TOC entry 2123 (class 2606 OID 143647)
-- Name: report_metadata report_metadata_pkey; Type: CONSTRAINT; Schema: core; Owner: -
--

ALTER TABLE ONLY report_metadata
    ADD CONSTRAINT report_metadata_pkey PRIMARY KEY (run_id);


--
-- TOC entry 2129 (class 2606 OID 143681)
-- Name: report_monthly_condition_stats report_monthly_condition_stats_pkey; Type: CONSTRAINT; Schema: core; Owner: -
--

ALTER TABLE ONLY report_monthly_condition_stats
    ADD CONSTRAINT report_monthly_condition_stats_pkey PRIMARY KEY (run_id, operator_id, cond_name);


--
-- TOC entry 2127 (class 2606 OID 143668)
-- Name: report_monthly_stats report_monthly_stats_pkey; Type: CONSTRAINT; Schema: core; Owner: -
--

ALTER TABLE ONLY report_monthly_stats
    ADD CONSTRAINT report_monthly_stats_pkey PRIMARY KEY (run_id, operator_id);


--
-- TOC entry 2135 (class 2606 OID 143720)
-- Name: report_monthly_top_models_gross_adds report_monthly_top_models_gross_adds_pkey; Type: CONSTRAINT; Schema: core; Owner: -
--

ALTER TABLE ONLY report_monthly_top_models_gross_adds
    ADD CONSTRAINT report_monthly_top_models_gross_adds_pkey PRIMARY KEY (run_id, operator_id, rank_pos);


--
-- TOC entry 2133 (class 2606 OID 143707)
-- Name: report_monthly_top_models_imei report_monthly_top_models_imei_pkey; Type: CONSTRAINT; Schema: core; Owner: -
--

ALTER TABLE ONLY report_monthly_top_models_imei
    ADD CONSTRAINT report_monthly_top_models_imei_pkey PRIMARY KEY (run_id, operator_id, rank_pos);


--
-- TOC entry 2137 (class 2606 OID 143726)
-- Name: schema_version schema_version_pkey; Type: CONSTRAINT; Schema: core; Owner: -
--

ALTER TABLE ONLY schema_version
    ADD CONSTRAINT schema_version_pkey PRIMARY KEY (version);


--
-- TOC entry 2139 (class 2606 OID 143750)
-- Name: stolen_list stolen_list_pkey; Type: CONSTRAINT; Schema: core; Owner: -
--

ALTER TABLE ONLY stolen_list
    ADD CONSTRAINT stolen_list_pkey PRIMARY KEY (imei);


--
-- TOC entry 2110 (class 1259 OID 143598)
-- Name: classification_state_block_date_idx; Type: INDEX; Schema: core; Owner: -
--

CREATE INDEX classification_state_block_date_idx ON classification_state USING btree (block_date) WHERE (end_date IS NULL);


--
-- TOC entry 2111 (class 1259 OID 143597)
-- Name: classification_state_cond_name_idx; Type: INDEX; Schema: core; Owner: -
--

CREATE INDEX classification_state_cond_name_idx ON classification_state USING btree (cond_name) WHERE (end_date IS NULL);


--
-- TOC entry 2112 (class 1259 OID 143596)
-- Name: classification_state_imei_norm_cond_name_idx; Type: INDEX; Schema: core; Owner: -
--

CREATE UNIQUE INDEX classification_state_imei_norm_cond_name_idx ON classification_state USING btree (imei_norm, cond_name) WHERE (end_date IS NULL);


--
-- TOC entry 2119 (class 1259 OID 143636)
-- Name: pairing_list_imei_idx; Type: INDEX; Schema: core; Owner: -
--

CREATE INDEX pairing_list_imei_idx ON pairing_list USING btree (imei_norm);


--
-- TOC entry 2145 (class 2620 OID 143734)
-- Name: seen_imeis seen_imeis_trigger; Type: TRIGGER; Schema: core; Owner: -
--

CREATE TRIGGER seen_imeis_trigger BEFORE INSERT ON seen_imeis FOR EACH ROW EXECUTE PROCEDURE fail_on_seen_imeis_table_insert();


--
-- TOC entry 2146 (class 2620 OID 143742)
-- Name: seen_triplets seen_triplets_trigger; Type: TRIGGER; Schema: core; Owner: -
--

CREATE TRIGGER seen_triplets_trigger BEFORE INSERT ON seen_triplets FOR EACH ROW EXECUTE PROCEDURE fail_on_seen_triplets_table_insert();


--
-- TOC entry 2143 (class 2606 OID 143695)
-- Name: report_blacklist_violation_stats report_blacklist_violation_stats_run_id_fkey; Type: FK CONSTRAINT; Schema: core; Owner: -
--

ALTER TABLE ONLY report_blacklist_violation_stats
    ADD CONSTRAINT report_blacklist_violation_stats_run_id_fkey FOREIGN KEY (run_id) REFERENCES report_metadata(run_id);


--
-- TOC entry 2140 (class 2606 OID 143656)
-- Name: report_daily_stats report_daily_stats_run_id_fkey; Type: FK CONSTRAINT; Schema: core; Owner: -
--

ALTER TABLE ONLY report_daily_stats
    ADD CONSTRAINT report_daily_stats_run_id_fkey FOREIGN KEY (run_id) REFERENCES report_metadata(run_id);


--
-- TOC entry 2142 (class 2606 OID 143682)
-- Name: report_monthly_condition_stats report_monthly_condition_stats_run_id_fkey; Type: FK CONSTRAINT; Schema: core; Owner: -
--

ALTER TABLE ONLY report_monthly_condition_stats
    ADD CONSTRAINT report_monthly_condition_stats_run_id_fkey FOREIGN KEY (run_id) REFERENCES report_metadata(run_id);


--
-- TOC entry 2141 (class 2606 OID 143669)
-- Name: report_monthly_stats report_monthly_stats_run_id_fkey; Type: FK CONSTRAINT; Schema: core; Owner: -
--

ALTER TABLE ONLY report_monthly_stats
    ADD CONSTRAINT report_monthly_stats_run_id_fkey FOREIGN KEY (run_id) REFERENCES report_metadata(run_id);


--
-- TOC entry 2144 (class 2606 OID 143708)
-- Name: report_monthly_top_models_imei report_monthly_top_models_imei_run_id_fkey; Type: FK CONSTRAINT; Schema: core; Owner: -
--

ALTER TABLE ONLY report_monthly_top_models_imei
    ADD CONSTRAINT report_monthly_top_models_imei_run_id_fkey FOREIGN KEY (run_id) REFERENCES report_metadata(run_id);


-- Completed on 2017-05-16 16:11:25 AEST

--
-- PostgreSQL database dump complete
--

