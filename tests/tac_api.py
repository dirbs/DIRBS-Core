"""
tac api data import unit tests.

Copyright (c) 2018 Qualcomm Technologies, Inc.

 All rights reserved.



 Redistribution and use in source and binary forms, with or without modification, are permitted (subject to the
 limitations in the disclaimer below) provided that the following conditions are met:


 * Redistributions of source code must retain the above copyright notice, this list of conditions and the following
 disclaimer.

 * Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following
 disclaimer in the documentation and/or other materials provided with the distribution.

 * Neither the name of Qualcomm Technologies, Inc. nor the names of its contributors may be used to endorse or promote
 products derived from this software without specific prior written permission.

 NO EXPRESS OR IMPLIED LICENSES TO ANY PARTY'S PATENT RIGHTS ARE GRANTED BY THIS LICENSE. THIS SOFTWARE IS PROVIDED BY
 THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
 THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
 COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR
 TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 POSSIBILITY OF SUCH DAMAGE.
"""

import json

from flask import url_for
import pytest

from _fixtures import *  # noqa: F403, F401
from _importer_params import GSMADataParams


def test_long_short_and_non_numeric_tac(flask_app, api_version):
    """Test Depot ID 96788/5.

    Verify that TAC API returns a 400 status for short and non-numeric,
    shorter and longer tacs.
    """
    if api_version == 'v1':
        # non-numeric tacs
        for t in ['abc', '1abc', 'abcdefgh', '1234ABCD', '12345678ABSDEF']:
            rv = flask_app.get(url_for('{0}.tac_api'.format(api_version), tac=t))
            assert rv.status_code == 400
            assert b'Bad TAC format' in rv.data

        # tacs less than 8 chars long
        for t in ['1', '00', '1234567']:
            rv = flask_app.get(url_for('{0}.tac_api'.format(api_version), tac=t))
            assert rv.status_code == 400
            assert b'Bad TAC format' in rv.data

        # tacs longer than 8 chars long
        for t in ['123456789', '012345678', '0123456780']:
            rv = flask_app.get(url_for('{0}.tac_api'.format(api_version), tac=t))
            assert rv.status_code == 400
            assert b'Bad TAC format' in rv.data
    else:  # api version 2
        # non-numeric tacs for tac get api
        non_numeric_tacs = ['abc', '1abc', 'abcdefgh', '1234ABCD', '12345678ABSDEF']
        for t in non_numeric_tacs:
            rv = flask_app.get(url_for('{0}.tac_get_api'.format(api_version), tac=t))
            assert rv.status_code == 400

        # non-numeric tacs for tac post api
        headers = {'content-type': 'application/json'}
        rv = flask_app.post(url_for('{0}.tac_post_api'.format(api_version)),
                            data=json.dumps({'tacs': non_numeric_tacs}), headers=headers)
        assert rv.status_code == 400
        assert b'Bad TAC format' in rv.data

        # tacs less than 8 chars long
        invalid_tacs = ['1', '00', '1234567']
        for t in invalid_tacs:
            rv = flask_app.get(url_for('{0}.tac_get_api'.format(api_version), tac=t))
            assert rv.status_code == 400
            assert b'Bad TAC format' in rv.data

        # tacs less than 8 chars long for post api
        headers = {'content-type': 'application/json'}
        rv = flask_app.post(url_for('{0}.tac_post_api'.format(api_version)),
                            data=json.dumps({'tacs': invalid_tacs}), headers=headers)
        assert rv.status_code == 400
        assert b'Bad TAC format' in rv.data

        # tacs longer than 8 chars long
        invalid_tacs = ['123456789', '012345678', '0123456780']
        for t in invalid_tacs:
            rv = flask_app.get(url_for('{0}.tac_get_api'.format(api_version), tac=t))
            assert rv.status_code == 400
            assert b'Bad TAC format' in rv.data

        # tacs longer than 8 chars for post api
        headers = {'content-type': 'application/json'}
        rv = flask_app.post(url_for('{0}.tac_post_api'.format(api_version)),
                            data=json.dumps({'tacs': invalid_tacs}), headers=headers)
        assert rv.status_code == 400
        assert b'Bad TAC format' in rv.data


def test_empty_tac(flask_app, api_version):
    """Test Depot ID 96557/8.

    Verify TAC API return a 404 status for a zero-length tac.
    """
    if api_version == 'v1':
        rv = flask_app.get(url_for('{0}.tac_api'.format(api_version), tac=''))
        assert rv.status_code == 404
    else:  # api version 2
        rv = flask_app.get(url_for('{0}.tac_get_api'.format(api_version), tac=''))
        assert rv.status_code == 404

        # empty tacs for post api
        empty_tacs = ['', '', '', '']
        headers = {'content-type': 'application/json'}
        rv = flask_app.post(url_for('{0}.tac_post_api'.format(api_version)),
                            data=json.dumps({'tacs': empty_tacs}), headers=headers)
        assert rv.status_code == 400
        assert b'Bad TAC format' in rv.data

        # one empty tac for post api
        empty_tacs = ['']
        rv = flask_app.post(url_for('{0}.tac_post_api'.format(api_version)),
                            data=json.dumps({'tacs': empty_tacs}), headers=headers)
        assert rv.status_code == 400
        assert b'Bad TAC format' in rv.data


def test_valid_missing_tac(flask_app, api_version):
    """Test Depot ID 96558/6.

    Verify that TAC API should return a 200 status for valid
    tacs that are not in GSMA, but the GSMA field should be null.
    """
    missing_tac = '12345678'
    if api_version == 'v1':
        rv = flask_app.get(url_for('{0}.tac_api'.format(api_version), tac='12345678'))
        assert rv.status_code == 200
        data = json.loads(rv.data.decode('utf-8'))
        assert data['tac'] == missing_tac
        assert data['gsma'] is None
    else:  # api version 2
        rv = flask_app.get(url_for('{0}.tac_get_api'.format(api_version), tac=missing_tac))
        assert rv.status_code == 200
        data = json.loads(rv.data.decode('utf-8'))
        assert data['tac'] == missing_tac
        assert data['gsma'] is None

        # valid more than one missing tacs for post api
        missing_tacs = ['12345678', '12345677']
        headers = {'content-type': 'application/json'}
        rv = flask_app.post(url_for('{0}.tac_post_api'.format(api_version)),
                            data=json.dumps({'tacs': missing_tacs}), headers=headers)
        assert rv.status_code == 200
        data = json.loads(rv.data.decode('utf-8'))
        assert len(data['results']) == 2

        # only one missing valid tac for post api
        missing_tacs = ['12345678']
        rv = flask_app.post(url_for('{0}.tac_post_api'.format(api_version)),
                            data=json.dumps({'tacs': missing_tacs}), headers=headers)
        assert rv.status_code == 200
        data = json.loads(rv.data.decode('utf-8'))
        assert data['results'][0]['tac'] == missing_tacs[0]
        assert data['results'][0]['gsma'] is None


@pytest.mark.parametrize('gsma_tac_db_importer',
                         [GSMADataParams(
                             filename='testData1-gsmatac_operator1_operator4_anonymized.txt')],
                         indirect=True)
def test_valid_tac(flask_app, gsma_tac_db_importer, api_version):
    """Test Depot ID 96559/7 - 96553/1.

    Verify that TAC API should return correct GSMA data
    for a known TAC (38826033).
    """
    gsma_tac_db_importer.import_data()
    valid_tac = '38826033'

    if api_version == 'v1':
        rv = flask_app.get(url_for('{0}.tac_api'.format(api_version), tac=valid_tac))
        assert rv.status_code == 200
        data = json.loads(rv.data.decode('utf-8'))
        assert data['tac'] == valid_tac
        gsma_data = data['gsma']
        assert gsma_data is not None
        assert gsma_data == {
            'allocation_date': '2001-01-01',
            'bands': '79786815f4e5ab4e775c44f4e5aa237c52147d75',
            'bluetooth': 'Not Known',
            'brand_name': '6fefdf8fdf21220bd9a56f58c1134b33c4e75a40',
            'country_code': '01592d51db5afd0165cb73baca5c0b340c4889f1',
            'device_type': 'a7920de2f4e1473556e8f373b8312a1c3044ef1c',
            'fixed_code': '187a332db248392c0ce1501765301bc6cf780b10',
            'internal_model_name': 'f3e808a9e81ac355d0e86b08a4e35953f14381ef',
            'manufacturer': 'ec307432a3d742bc70041ab01f6740a57f34ba53',
            'manufacturer_code': 'd91c5ff622f821641812336fcc7d964b5f80a0a3',
            'marketing_name': 'f3e808a9e81ac355d0e86b08a4e35953f14381ef',
            'model_name': 'f3e808a9e81ac355d0e86b08a4e35953f14381ef',
            'nfc': 'Y',
            'operating_system': '6fefdf8fdf21220bd9a56f58c1134b33c4e75a40',
            'radio_interface': '0654a028e5aea48c8fbb09871b8f397a186c883b',
            'wlan': 'N'
        }
    elif api_version == 'v2':
        rv = flask_app.get(url_for('{0}.tac_get_api'.format(api_version), tac=valid_tac))
        assert rv.status_code == 200
        data = json.loads(rv.data.decode('utf-8'))
        assert data['tac'] == valid_tac
        gsma_data = data['gsma']
        assert gsma_data is not None

        assert gsma_data == {
            'allocation_date': '2001-01-01',
            'bands': '79786815f4e5ab4e775c44f4e5aa237c52147d75',
            'bluetooth': 'Not Known',
            'brand_name': '6fefdf8fdf21220bd9a56f58c1134b33c4e75a40',
            'device_type': 'a7920de2f4e1473556e8f373b8312a1c3044ef1c',
            'internal_model_name': 'f3e808a9e81ac355d0e86b08a4e35953f14381ef',
            'manufacturer': 'ec307432a3d742bc70041ab01f6740a57f34ba53',
            'marketing_name': 'f3e808a9e81ac355d0e86b08a4e35953f14381ef',
            'model_name': 'f3e808a9e81ac355d0e86b08a4e35953f14381ef',
            'nfc': 'Y',
            'radio_interface': '0654a028e5aea48c8fbb09871b8f397a186c883b',
            'wlan': 'N'
        }


@pytest.mark.parametrize('gsma_tac_db_importer',
                         [GSMADataParams(
                             filename='testData1-gsmatac_operator1_operator4_anonymized.txt')],
                         indirect=True)
def test_batch_tacs(flask_app, gsma_tac_db_importer, api_version):
    """
    Verify TAC batch API.

    Verify that TAC API should return correct GSMA data
    for a batch of known tacs (21260934, 21782434, 38245933, 38709433).
    """
    gsma_tac_db_importer.import_data()
    valid_tacs = {
        'tacs': [
            '21260934',
            '21782434',
            '38245933',
            '38709433'
        ]
    }

    if api_version == 'v1':
        rv = flask_app.post(url_for('{0}.tac_api'.format(api_version), tac='12345678'))
        assert rv.status_code == 405
        assert b'Method Not Allowed' in rv.data
    else:  # api version 2
        rv = flask_app.post(url_for('{0}.tac_post_api'.format(api_version)), data=json.dumps(valid_tacs),
                            content_type='application/json')
        data = json.loads(rv.data.decode('utf-8'))

        assert rv.status_code == 200
        assert data is not None
        assert len(data['results']) == 4

        for item in data['results']:
            gsma_data = item.get('gsma')

            assert gsma_data is not None
            assert item.get('tac') in valid_tacs.get('tacs')

        # batch tacs limit
        tacs = ['00100100', '00100200', '00100300', '00100400', '00100500', '00100600', '00100700', '00100800',
                '00100900', '00101000', '00101300', '00101500', '00101600', '00101700', '00101710', '00101800',
                '00102000', '00102100', '00102200', '00102300', '00102400', '00102500', '00102600', '00102700',
                '00102800', '00102900', '00103000', '00103100', '00103200', '00103300', '00103500', '00103600',
                '00103700', '00103800', '00103900', '00104100', '00104200', '00104400', '00104500', '00104600',
                '00104700', '00104800', '00105000', '00105200', '00105300', '00105400', '00105500', '00105600',
                '00105700', '00105900', '00106200',
                '00106400',
                '00106500',
                '00106600',
                '00106700',
                '00106900',
                '00107100',
                '00107200',
                '00107400',
                '00107600',
                '00107700',
                '00107800',
                '00107900',
                '00108000',
                '00108100',
                '00108200',
                '00108300',
                '00108400',
                '00110900',
                '00116000',
                '00440101',
                '00440102',
                '00440103',
                '00440104',
                '00440107',
                '00440108',
                '00440109',
                '00440110',
                '00440111',
                '00440112',
                '00440113',
                '00440114',
                '00440120',
                '00440121',
                '00440122',
                '00440123',
                '00440125',
                '00440126',
                '00440127',
                '00440128',
                '00440129',
                '00440130',
                '00440131',
                '00440132',
                '00440133',
                '00440134',
                '00440135',
                '00440141',
                '00440142',
                '00440143',
                '00440144',
                '00440145',
                '00440146',
                '00440147',
                '00440148',
                '00440149',
                '00440150',
                '00440151',
                '00440152',
                '00440153',
                '00440154',
                '00440155',
                '00440156',
                '00440157',
                '00440158',
                '00440159',
                '00440160',
                '00440161',
                '00440162',
                '00440163',
                '00440164',
                '00440165',
                '00440166',
                '00440167',
                '00440168',
                '00440169',
                '00440170',
                '00440171',
                '00440172',
                '00440173',
                '00440174',
                '00440175',
                '00440176',
                '00440177',
                '00440178',
                '00440179',
                '00440180',
                '00440181',
                '00440182',
                '00440183',
                '00440184',
                '00440185',
                '00440186',
                '00440187',
                '00440188',
                '00440189',
                '00440190',
                '00440191',
                '00440192',
                '00440193',
                '00440194',
                '00440195',
                '00440196',
                '00440197',
                '00440198',
                '00440199',
                '00440200',
                '00440201',
                '00440202',
                '00440203',
                '00440204',
                '00440205',
                '00440206',
                '00440207',
                '00440208',
                '00440209',
                '00440210',
                '00440211',
                '00440212',
                '00440213',
                '00440214',
                '00440215',
                '00440216',
                '00440217',
                '00440218',
                '00440219',
                '00440220',
                '00440221',
                '00440222',
                '00440223',
                '00440224',
                '00440225',
                '00440226',
                '00440227',
                '00440228',
                '00440229',
                '00440230',
                '00440231',
                '00440232',
                '00440233',
                '00440234',
                '00440235',
                '00440236',
                '00440237',
                '00440238',
                '00440239',
                '00440240',
                '00440241',
                '00440242',
                '00440243',
                '00440244',
                '00440245',
                '00440246',
                '00440247',
                '00440248',
                '00440249',
                '00440250',
                '00440251',
                '00440252',
                '00440253',
                '00440254',
                '00440255',
                '00440256',
                '00440257',
                '00440258',
                '00440259',
                '00440260',
                '00440261',
                '00440262',
                '00440263',
                '00440264',
                '00440265',
                '00440266',
                '00440267',
                '00440268',
                '00440269',
                '00440270',
                '00440271',
                '00440272',
                '00440273',
                '00440274',
                '00440275',
                '00440276',
                '00440277',
                '00440278',
                '00440279',
                '00440280',
                '00440281',
                '00440282',
                '00440283',
                '00440284',
                '00440285',
                '00440286',
                '00440287',
                '00440288',
                '00440289',
                '00440291',
                '00440292',
                '00440293',
                '00440294',
                '00440295',
                '00440296',
                '00440297',
                '00440298',
                '00440299',
                '00440300',
                '00440301',
                '00440302',
                '00440303',
                '00440304',
                '00440305',
                '00440306',
                '00440307',
                '00860006',
                '01002151',
                '01002152',
                '01002251',
                '01002272',
                '01002311',
                '01002312',
                '01002650',
                '01002751',
                '01002772',
                '01002810',
                '01002830',
                '01002850',
                '01002901',
                '01002902',
                '01003001',
                '01003002',
                '01003100',
                '01003167',
                '01003169',
                '01003200',
                '01003267',
                '01003269',
                '01003300',
                '01003367',
                '01003369',
                '01003400',
                '01003467',
                '01003469',
                '01003500',
                '01003567',
                '01003569',
                '01003652',
                '01003710',
                '01003730',
                '01003750',
                '01003800',
                '01003867',
                '01003869',
                '01003910',
                '01003935',
                '01003982',
                '01004051',
                '01004052',
                '01004151',
                '01004152',
                '01004211',
                '01004212',
                '01004310',
                '01004335',
                '01004382',
                '01004410',
                '01004435',
                '01004482',
                '01004510',
                '01004535',
                '01004582',
                '01004600',
                '01004667',
                '01004669',
                '01004700',
                '01004767',
                '01004769',
                '01004910',
                '01004930',
                '01004950',
                '01005111',
                '01005112',
                '01005200',
                '01005342',
                '01005351',
                '01005361',
                '01005371',
                '01005410',
                '01005430',
                '01005450',
                '01005510',
                '01005530',
                '01005550',
                '01005611',
                '01005612',
                '01005711',
                '01005712',
                '01006000',
                '01006100',
                '01006169',
                '01006200',
                '01006300',
                '01006310',
                '01006330',
                '01006350',
                '01006451',
                '01006452',
                '01006510',
                '01006601',
                '01006719',
                '01006751',
                '01006752',
                '01006760',
                '01006800',
                '01006819',
                '01006851',
                '01006852',
                '01006860',
                '01006900',
                '01007000',
                '01007100',
                '01007230',
                '01007231',
                '01007300',
                '01007511',
                '01007512',
                '01007600',
                '01007608',
                '01007700',
                '01007800',
                '01007900',
                '01008000',
                '01008130',
                '01008131',
                '01008201',
                '01008300',
                '01008421',
                '01008500',
                '01008600',
                '01008700',
                '01008800',
                '01008930',
                '01009030',
                '01009032',
                '01009100',
                '01009200',
                '01009330',
                '01009400',
                '01009568',
                '01009600',
                '01009601',
                '01009700',
                '01009800',
                '01009900',
                '01009901',
                '01009902',
                '01009903',
                '01009904',
                '01009905',
                '01009906',
                '01009907',
                '01009908',
                '01009909',
                '01009910',
                '01009911',
                '01009912',
                '01009913',
                '01009914',
                '01009915',
                '01009916',
                '01009917',
                '01009918',
                '01009919',
                '01010000',
                '01010100',
                '01010200',
                '01010300',
                '01010400',
                '01010500',
                '01010600',
                '01010700',
                '01010788',
                '01010800',
                '01011000',
                '01011083',
                '01011151',
                '01011152',
                '01011200',
                '01011300',
                '01011400',
                '01011430',
                '01011431',
                '01011432',
                '01011433',
                '01011434',
                '01011435',
                '01011436',
                '01011437',
                '01011438',
                '01011439',
                '01011500',
                '01011667',
                '01011668',
                '01011767',
                '01011768',
                '01011800',
                '01011900',
                '01012068',
                '01012069',
                '01012100',
                '01012200',
                '01012230',
                '01012232',
                '01012300',
                '01012400',
                '01012600',
                '01012751',
                '01012752',
                '01012830',
                '01012831',
                '01012832',
                '01012833',
                '01012834',
                '01012835',
                '01012836',
                '01012837',
                '01012838',
                '01012839',
                '01012900',
                '01012953',
                '01013000',
                '01013047',
                '01013062',
                '01013069',
                '01013085',
                '01013100',
                '01013151',
                '01013152',
                '01013200',
                '01013226',
                '01013300',
                '01013400',
                '01013451',
                '01013452',
                '01013500',
                '01013501',
                '01013630',
                '01013631',
                '01013632',
                '01013633',
                '01013634',
                '01013635',
                '01013636',
                '01013637',
                '01013638',
                '01013639',
                '01013650',
                '01013730',
                '01013731',
                '01013732',
                '01013733',
                '01013734',
                '01013735',
                '01013736',
                '01013737',
                '01013738',
                '01013739',
                '01013830',
                '01013831',
                '01013832',
                '01013833',
                '01013834',
                '01013835',
                '01013836',
                '01013837',
                '01013838',
                '01013839',
                '01013900',
                '01014050',
                '01014072',
                '01014100',
                '01014200',
                '01014269',
                '01014300',
                '01014450',
                '01014472',
                '01014530',
                '01014531',
                '01014532',
                '01014533',
                '01014534',
                '01014535',
                '01014550',
                '01014572',
                '01014610',
                '01014691',
                '01014700',
                '01014800',
                '01014900',
                '01015000',
                '01015100',
                '01015200',
                '01015311',
                '01015400',
                '01015500',
                '01015600',
                '01015651',
                '01015771',
                '01015871',
                '01015881',
                '01015900',
                '01016000',
                '01016100',
                '01016200',
                '01016300',
                '01016400',
                '01016500',
                '01016600',
                '01016700',
                '01016800',
                '01016900',
                '01017000',
                '01017100',
                '01017200',
                '01017300',
                '01017400',
                '01017500',
                '01017600',
                '01017700',
                '01017800',
                '01017900',
                '01018000',
                '01018100',
                '01018200',
                '01018300',
                '01018400',
                '01018500',
                '01018600',
                '01018700',
                '01018800',
                '01018900',
                '01019000',
                '01019100',
                '01019200',
                '01019300',
                '01019400',
                '01019500',
                '01019600',
                '01019700',
                '01019800',
                '01019900',
                '01020000',
                '01020100',
                '01020200',
                '01020300',
                '01020400',
                '01020500',
                '01020600',
                '01020700',
                '01020800',
                '01020900',
                '01021000',
                '01021030',
                '01021032',
                '01021100',
                '01021200',
                '01021300',
                '01021400',
                '01021500',
                '01021600',
                '01021700',
                '01021800',
                '01021900',
                '01022000',
                '01022100',
                '01022200',
                '01022300',
                '01022400',
                '01022500',
                '01022600',
                '01022700',
                '01022800',
                '01022900',
                '01023000',
                '01023100',
                '01023200',
                '01023300',
                '01023400',
                '01023500',
                '01023600',
                '01023700',
                '01023800',
                '01023900',
                '01024000',
                '01024100',
                '01024200',
                '01024300',
                '01024400',
                '01024600',
                '01024700',
                '01024800',
                '01024900',
                '01025000',
                '01025100',
                '01025200',
                '01025300',
                '01025400',
                '01025500',
                '01025600',
                '01025700',
                '01025800',
                '01025900',
                '01026000',
                '01026100',
                '01026200',
                '01026300',
                '01026400',
                '01026500',
                '01026600',
                '01026700',
                '01026800',
                '01026900',
                '01027000',
                '01027100',
                '01027200',
                '01027300',
                '01027400',
                '01027500',
                '01027600',
                '01027700',
                '01027800',
                '01027900',
                '01028000',
                '01028100',
                '01028200',
                '01028300',
                '01028400',
                '01028500',
                '01028600',
                '01028700',
                '01028800',
                '01028900',
                '01029000',
                '01029100',
                '01029200',
                '01029300',
                '01029400',
                '01029500',
                '01029600',
                '01029700',
                '01029800',
                '01029900',
                '01030000',
                '01030100',
                '01030200',
                '01030300',
                '01030400',
                '01030500',
                '01030600',
                '01030700',
                '01030800',
                '01030900',
                '01031000',
                '01031100',
                '01031200',
                '01031300',
                '01031400',
                '01031500',
                '01031600',
                '01031700',
                '01031800',
                '01031900',
                '01032000',
                '01032100',
                '01032200',
                '01032300',
                '01032400',
                '01032500',
                '01032600',
                '01032700',
                '01032800',
                '01032900',
                '01033000',
                '01033100',
                '01033200',
                '01033300',
                '01033400',
                '01033500',
                '01033600',
                '01033700',
                '01033800',
                '01033900',
                '01034000',
                '01034100',
                '01034200',
                '01034300',
                '01034400',
                '01034500',
                '01034600',
                '01034700',
                '01034800',
                '01034900',
                '01035000',
                '01035100',
                '01035200',
                '01035300',
                '01035400',
                '01035500',
                '01035600',
                '01035700',
                '01035800',
                '01035900',
                '01036000',
                '01036100',
                '01036200',
                '01036300',
                '01036400',
                '01036500',
                '01036600',
                '01036700',
                '01036800',
                '01036900',
                '01037000',
                '01037100',
                '01037200',
                '01037300',
                '01037400',
                '01037500',
                '01037600',
                '01037700',
                '01037800',
                '01037900',
                '01038000',
                '01038100',
                '01038200',
                '01038300',
                '01038400',
                '01038500',
                '01038600',
                '01038700',
                '01038800',
                '01038900',
                '01039000',
                '01039100',
                '01039200',
                '01039300',
                '01039400',
                '01039500',
                '01039600',
                '01039700',
                '01039800',
                '01039900',
                '01040000',
                '01040100',
                '01040200',
                '01040300',
                '01040400',
                '01040500',
                '01040600',
                '01040700',
                '01040800',
                '01040900',
                '01041000',
                '01041100',
                '01041200',
                '01041300',
                '01041400',
                '01041500',
                '01041600',
                '01041700',
                '01041800',
                '01041900',
                '01042000',
                '01042100',
                '01042200',
                '01042300',
                '01042400',
                '01042500',
                '01042600',
                '01042700',
                '01042800',
                '01042900',
                '01043000',
                '01043200',
                '01043300',
                '01043400',
                '01043500',
                '01043600',
                '01043700',
                '01043800',
                '01043900',
                '01044000',
                '01044100',
                '01044200',
                '01044300',
                '01044400',
                '01044500',
                '01044600',
                '01044700',
                '01044800',
                '01044900',
                '01045000',
                '01045100',
                '01045200',
                '01045300',
                '01045400',
                '01045500',
                '01045600',
                '01045700',
                '01045800',
                '01045900',
                '01046000',
                '01046100',
                '01046200',
                '01046300',
                '01046400',
                '01046500',
                '01046600',
                '01046700',
                '01046800',
                '01046900',
                '01047000',
                '01047100',
                '01047200',
                '01047300',
                '01047400',
                '01047500',
                '01047600',
                '01047700',
                '01047800',
                '01047900',
                '01048000',
                '01048100',
                '01048200',
                '01048300',
                '01048400',
                '01048500',
                '01048600',
                '01048700',
                '01048800',
                '01048900',
                '01049000',
                '01049100',
                '01049200',
                '01049300',
                '01049400',
                '01049500',
                '01049600',
                '01049700',
                '01049800',
                '01049900',
                '01050000',
                '01050100',
                '01050200',
                '01050300',
                '01050400',
                '01050500',
                '01050600',
                '01050700',
                '01050800',
                '01050900',
                '01051000',
                '01051100',
                '01051200',
                '01051300',
                '01051400',
                '01051500',
                '01051600',
                '01051700',
                '01051800',
                '01051900',
                '01052000',
                '01052100',
                '01052200',
                '01052300',
                '01052400',
                '01052600',
                '01052700',
                '01052800',
                '01052900',
                '01053000',
                '01053100',
                '01053200',
                '01053300',
                '01053400',
                '01053500',
                '01053600',
                '01053700',
                '01053800',
                '01053900',
                '01054000',
                '01054100',
                '01054200',
                '01054300',
                '01054400',
                '01054500',
                '01054600',
                '01054700',
                '01054800',
                '01054900',
                '01055000',
                '01055100',
                '01055200',
                '01055300',
                '01055400',
                '01055500',
                '01055600',
                '01055700',
                '01055800',
                '01055900',
                '01056000',
                '01056100',
                '01056200',
                '01056300',
                '01056400',
                '01056500',
                '01056600',
                '01056700',
                '01056800',
                '01056900',
                '01057000',
                '01057100',
                '01057200',
                '01057300',
                '01057400',
                '01057500',
                '01057600',
                '01057700',
                '01057800',
                '01057900',
                '01058000',
                '01058100',
                '01058200',
                '01058300',
                '01058400',
                '01058500',
                '01058600',
                '01058700',
                '01058800',
                '01058900',
                '01059000',
                '01059100',
                '01059200',
                '01059300',
                '01059400',
                '01059500',
                '01059600',
                '01059700',
                '01059800',
                '01059900',
                '01060000',
                '01060100',
                '01060200',
                '01060300',
                '01060400',
                '01060500']
        rv = flask_app.post(url_for('{0}.tac_post_api'.format(api_version)), data=json.dumps({'tacs': tacs}),
                            content_type='application/json')
        assert rv.status_code == 400
        assert b'Bad TAC Input format' in rv.data


def test_method_put_not_allowed(flask_app, api_version):
    """Test Depot ID 96554/2.

    Verify the TAC API does not support HTTP PUT and returns HTTP 405 METHOD NOT ALLOWED.
    """
    if api_version == 'v1':
        rv = flask_app.put(url_for('{0}.tac_api'.format(api_version), tac='35567907'))
        assert rv.status_code == 405
        assert b'Method Not Allowed' in rv.data
    else:  # api version 2
        rv = flask_app.put(url_for('{0}.tac_get_api'.format(api_version), tac='35567907'))
        assert rv.status_code == 405
        assert b'Method Not Allowed' in rv.data

        headers = {'content-type': 'application/json'}
        data = ['12345678', '12345678']
        rv = flask_app.put(url_for('{0}.tac_post_api'.format(api_version)),
                           data=json.dumps({'tacs': data}), headers=headers)
        assert rv.status_code == 405
        assert b'Method Not Allowed' in rv.data


def test_method_post_not_allowed(flask_app, api_version):
    """Test Depot ID 96555/3.

    Verify the TAC API does not support HTTP POST and returns HTTP 405 METHOD NOT ALLOWED.
    """
    if api_version == 'v1':
        rv = flask_app.post(url_for('{0}.tac_api'.format(api_version), tac='35567907'))
        assert rv.status_code == 405
        assert b'Method Not Allowed' in rv.data
    else:  # api version 2, method allowed
        rv = flask_app.post(url_for('{0}.tac_get_api'.format(api_version), tac='35567907'))
        assert rv.status_code == 405
        assert b'Method Not Allowed' in rv.data


def test_method_delete_not_allowed(flask_app, api_version):
    """Test Depot ID 96556/3.

    Verify the TAC API does not support HTTP DELETE and returns HTTP 405 METHOD NOT ALLOWED.
    """
    if api_version == 'v1':
        rv = flask_app.delete(url_for('{0}.tac_api'.format(api_version), tac='35567907'))
        assert rv.status_code == 405
        assert b'Method Not Allowed' in rv.data
    else:  # api version 2
        rv = flask_app.delete(url_for('{0}.tac_get_api'.format(api_version), tac='35567907'))
        assert rv.status_code == 405
        assert b'Method Not Allowed' in rv.data

        headers = {'content-type': 'application/json'}
        rv = flask_app.delete(url_for('{0}.tac_post_api'.format(api_version)), headers=headers)
        assert rv.status_code == 405
        assert b'Method Not Allowed' in rv.data


def test_same_tacs_batch_tac(flask_app):
    """Test Depot ID not known.

    Verify that when same tacs are entered to batch tac api, it returns respnse for unique tacs.
    """
    tacs = ['12345678', '12345678', '22222222', '11111111', '11111111']
    headers = {'content-type': 'application/json'}
    rv = flask_app.post(url_for('v2.tac_post_api'), data=json.dumps({'tacs': tacs}), headers=headers,
                        content_type='application/json')
    assert rv.status_code == 200
    data = json.loads(rv.data.decode('utf-8')).get('results')
    assert len(data) == 3

    for item in data:
        assert item.get('tac') in tacs
