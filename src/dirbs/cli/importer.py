"""
DIRBS CLI for data import. Installed by setuptools as a dirbs-import console script.

Copyright (c) 2018-2021 Qualcomm Technologies, Inc.

All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted (subject to the
limitations in the disclaimer below) provided that the following conditions are met:

- Redistributions of source code must retain the above copyright notice, this list of conditions and the following
  disclaimer.
- Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following
  disclaimer in the documentation and/or other materials provided with the distribution.
- Neither the name of Qualcomm Technologies, Inc. nor the names of its contributors may be used to endorse or promote
  products derived from this software without specific prior written permission.
- The origin of this software must not be misrepresented; you must not claim that you wrote the original software.
  If you use this software in a product, an acknowledgment is required by displaying the trademark/logo as per the
  details provided here: https://www.qualcomm.com/documents/dirbs-logo-and-brand-guidelines
- Altered source versions must be plainly marked as such, and must not be misrepresented as being the original
  software.
- This notice may not be removed or altered from any source distribution.

NO EXPRESS OR IMPLIED LICENSES TO ANY PARTY'S PATENT RIGHTS ARE GRANTED BY THIS LICENSE. THIS SOFTWARE IS PROVIDED BY
THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
POSSIBILITY OF SUCH DAMAGE.
"""

import logging
import sys
from functools import wraps

import click

import dirbs.cli.common as common
from dirbs.importer.exceptions import ImportCheckException
from dirbs.importer import importer_factory


def _process_batch_size(ctx, param, val):
    """
    Process batch size cli option.

    :param ctx: current cli context obj
    :param param: param
    :param val: batch size value
    :return: batch size
    """
    logger = logging.getLogger('dirbs.import')
    config = common.ensure_config(ctx)
    if val is not None:
        if val < 0:
            logger.warning('Ignoring invalid value %d for --batch-size', val)
        config.import_config.batch_size = val
    return val


def _process_disable_msisdn_import(ctx, param, val):
    """
    Process disable msisdn import cli option.

    :param ctx: current cli context obj
    :param param: param
    :param val: flag value
    :return: flag value
    """
    config = common.ensure_config(ctx)
    if val is not None:
        config.region_config.import_msisdn_data = not val
    return val


def _process_disable_rat_import(ctx, param, val):
    """
    Process disable rat import cli option.

    :param ctx: current cli context obj
    :param param: param
    :param val: flag value
    :return: flag value
    """
    config = common.ensure_config(ctx)
    if val is not None:
        config.region_config.import_rat_data = not val
    return val


def _validate_operator_id(ctx, param, val):
    """
    Process and validate operator id.

    :param ctx: current cli context obj
    :param param: param
    :param val: operator id
    :return: validated operator id
    """
    logger = logging.getLogger('dirbs.import')
    if len(val) > 16:
        raise click.BadParameter('Operator ID must be 16 chars or less')
    else:
        config = common.ensure_config(ctx)
        operator_id_list = [op.id for op in config.region_config.operators]
        if val.lower() not in operator_id_list:
            raise click.BadParameter("\'{0}\' not in {1}".format(val, operator_id_list))
        elif val != val.lower():
            logger.warning('operator_id: {0} has been changed to lower case: {1}'.format(val, val.lower()))
    return val.lower()


def _validate_input_file_extension(ctx, param, input_file):
    """
    Use file extension to blind check if a zip file is provided as input.

    Importer file extractor does a full check by opening the file and trying to extract the zip contents.
    zipfile.is_zipfile() function is not reliable as .xlsx files are also passed through for example.

    :param ctx: current cli context obj
    :param param: param
    :param input_file: input file
    :return: input file
    """
    if input_file.lower()[-3:] != 'zip':
        raise click.BadParameter('{0} does not have the correct file extension (.zip)'.format(input_file))
    return input_file


def disable_historic_check_option(f):
    """
    Function to parse the verbosity flag used by all CLI programs.

    :param f: flag obj
    :return: cli option obj
    """
    return click.option('--disable-historic-check',
                        default=False,
                        is_flag=True,
                        help='Skip checking the size of this import against the currently stored data.')(f)


def _enable_delta_import_mode(f):
    """
    Function to switch importer to delta import mode.

    :param f: obj
    :return: options obj
    """
    return click.option('--delta',
                        default=False,
                        is_flag=True,
                        help='Switch to delta import mode.')(f)


def _disable_delta_adds_check(f):
    """
    Function to disable verification that delta adds are not already in the DB.

    :param f: obj
    :return: options obj
    """
    return click.option('--disable-delta-adds-check',
                        default=False,
                        is_flag=True,
                        help='If in delta mode, disable verification that adds in delta list '
                             'are not already in DB.')(f)


def _disable_delta_removes_check(f):
    """
    Function to disable verification that delta removes are already in the DB.

    :param f: obj
    :return: options obj
    """
    return click.option('--disable-delta-removes-check',
                        default=False,
                        is_flag=True,
                        help='If in delta mode, disable verification that removes in delta list are already in DB.')(f)


def _disable_delta_updates_check(f):
    """
    Function to disable delta check for updates.

    :param f: obj
    :return: options obj
    """
    return click.option('--disable-delta-updates-check',
                        default=False,
                        is_flag=True,
                        help='If in delta mode, disable verification that updates in delta list are already in DB.')(f)


def add_delta_options(f):
    """
    Decorator used to parse all the delta validation check options.

    :param f: obj
    :return: obj
    """
    f = _enable_delta_import_mode(f)
    f = _disable_delta_updates_check(f)
    f = _disable_delta_removes_check(f)
    f = _disable_delta_adds_check(f)
    return f


def handle_import_check_exception(f):
    """
    Makes sure that any import check exception is handled here so it is not treated as an uncaught exception.

    :param f: obj
    :return: decorated obj
    """
    @wraps(f)
    def decorated(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except ImportCheckException as ex:
            logger = logging.getLogger('dirbs.import')
            logger.error(str(ex))
            sys.exit(1)

    return decorated


@click.group(no_args_is_help=False)
@common.setup_initial_logging
@click.version_option()
@common.parse_verbosity_option
@common.parse_db_options
@common.parse_statsd_options
@common.parse_multiprocessing_options
@click.option('--batch-size',
              type=int,
              help='Size of batches to import into DB, in lines.',
              callback=_process_batch_size,
              expose_value=False)
@click.option('--no-cleanup',
              is_flag=True,
              help='If set, intermediate split data files and the staging table will not be deleted so that '
                   'they can be inspected.')
@click.option('--extract-dir',
              type=click.Path(exists=True, file_okay=False, writable=True),
              default=None,
              help='Directory to extract contents of .zip file into (same directory as input file by default).')
@click.option('--prevalidator-path',
              type=click.Path(exists=True, dir_okay=False),
              help='The path to the CSV pre-validator executable.',
              default='/opt/validator/bin/validate')
@click.option('--prevalidator-schema-path',
              type=click.Path(exists=True, dir_okay=True, file_okay=False),
              help='The path to the directory where the CSV pre-validator schema are stored.',
              default='/opt/dirbs/etc/schema')
@click.pass_context
@common.configure_logging
def cli(ctx, no_cleanup, extract_dir, prevalidator_path, prevalidator_schema_path):
    """DIRBS script to import data into DIRBS Core PostgreSQL database."""
    ctx.obj['NO_CLEANUP'] = no_cleanup
    ctx.obj['EXTRACT_DIR'] = extract_dir
    ctx.obj['PREVALIDATOR_PATH'] = prevalidator_path
    ctx.obj['PREVALIDATOR_SCHEMA_PATH'] = prevalidator_schema_path


def _common_import_params(ctx):
    """
    Dictionary containing parameters provided to the dirbs-import command.

    :param ctx: current cli context obj
    :return: dict
    """
    return {'prevalidator_path': ctx.obj['PREVALIDATOR_PATH'],
            'prevalidator_schema_path': ctx.obj['PREVALIDATOR_SCHEMA_PATH'],
            'no_cleanup': ctx.obj['NO_CLEANUP'],
            'extract_dir': ctx.obj['EXTRACT_DIR']}


@cli.command()
@click.argument('operator_id', callback=_validate_operator_id)
@click.argument('input_file', type=click.Path(exists=True, dir_okay=False), callback=_validate_input_file_extension)
@click.option('--disable-leading-zero-check',
              default=False,
              is_flag=True,
              help='Skip checking if the import data appears to have lost leading zeros.')
@click.option('--disable-null-check',
              default=False,
              is_flag=True,
              help='Skip checking the ratio of IMSIs, MSISDNs, IMEIs and RATs that are NULL.')
@click.option('--disable-clean-check',
              default=False,
              is_flag=True,
              help='Skip checking the ratio of IMEIs and IMSIs that are the wrong length or contain invalid '
                   'characters.')
@click.option('--disable-region-check',
              default=False,
              is_flag=True,
              help='Skip checking the ratio of MSISDNs and IMSIs that have out of region cc and mcc values.')
@click.option('--disable-home-check',
              default=False,
              is_flag=True,
              help='Skip checking the ratio of and IMSIs that have out of region mcc and mnc pair values.')
@click.option('--disable-msisdn-import',
              default=False,
              is_flag=True,
              help='Skip importing MSISDN field even if it does exist in input data.',
              callback=_process_disable_msisdn_import,
              expose_value=False)
@click.option('--disable-rat-import',
              default=False,
              is_flag=True,
              help='Skip importing RAT field if it does not exist in input data.',
              callback=_process_disable_rat_import,
              expose_value=False)
@click.option('--disable-auto-analyze',
              default=False,
              is_flag=True,
              help='Skip auto analyzing of historic tables associated with operator data import')
@disable_historic_check_option
@click.pass_context
@common.unhandled_exception_handler
@handle_import_check_exception
@common.cli_wrapper(command='dirbs-import', subcommand='operator', required_role='dirbs_core_import_operator',
                    metrics_root=lambda ctx, *args, **kwargs:
                        'dirbs.import.operator.{0}'.format(ctx.params['operator_id'].lower() + '.'))
def operator(ctx, config, statsd, logger, run_id, conn, metadata_conn, command, metrics_root, metrics_run_root,
             operator_id, input_file, disable_leading_zero_check, disable_null_check, disable_clean_check,
             disable_region_check, disable_home_check, disable_historic_check, disable_auto_analyze):
    """
    Import the CSV operator data found in INPUT into the PostgreSQL database.

    OPERATOR_ID is an ID up to 16 characters to unique identify the operator.
    """
    op_tc = config.operator_threshold_config
    params = _common_import_params(ctx)
    params.update({'operator_id': operator_id,
                   'null_imei_threshold': op_tc.null_imei_threshold,
                   'null_imsi_threshold': op_tc.null_imsi_threshold,
                   'null_msisdn_threshold': op_tc.null_msisdn_threshold,
                   'null_rat_threshold': op_tc.null_rat_threshold,
                   'null_threshold': op_tc.null_threshold,
                   'unclean_imei_threshold': op_tc.unclean_imei_threshold,
                   'unclean_imsi_threshold': op_tc.unclean_imsi_threshold,
                   'unclean_threshold': op_tc.unclean_threshold,
                   'out_of_region_imsi_threshold': op_tc.out_of_region_imsi_threshold,
                   'out_of_region_msisdn_threshold': op_tc.out_of_region_msisdn_threshold,
                   'out_of_region_threshold': op_tc.out_of_region_threshold,
                   'non_home_network_threshold': op_tc.non_home_network_threshold,
                   'historic_imei_threshold': op_tc.historic_imei_threshold,
                   'historic_imsi_threshold': op_tc.historic_imsi_threshold,
                   'historic_msisdn_threshold': op_tc.historic_msisdn_threshold,
                   'perform_msisdn_import': config.region_config.import_msisdn_data,
                   'perform_rat_import': config.region_config.import_rat_data,
                   'perform_leading_zero_check': not disable_leading_zero_check,
                   'perform_null_checks': not disable_null_check,
                   'perform_unclean_checks': not disable_clean_check,
                   'perform_region_checks': not disable_region_check,
                   'perform_home_network_check': not disable_home_check,
                   'perform_historic_checks': not disable_historic_check,
                   'perform_auto_analyze': not disable_auto_analyze,
                   'leading_zero_suspect_limit': op_tc.leading_zero_suspect_limit})
    with importer_factory.make_data_importer('operator', input_file, config, statsd, conn, metadata_conn,
                                             run_id, metrics_root, metrics_run_root, **params) as importer:
        importer.import_data()


@cli.command(name='gsma_tac')
@click.argument('input_file', type=click.Path(exists=True, dir_okay=False), callback=_validate_input_file_extension)
@disable_historic_check_option
@click.pass_context
@common.unhandled_exception_handler
@handle_import_check_exception
@common.cli_wrapper(command='dirbs-import', subcommand='gsma_tac', required_role='dirbs_core_import_gsma')
def gsma_tac(ctx, config, statsd, logger, run_id, conn, metadata_conn, command, metrics_root, metrics_run_root,
             input_file, disable_historic_check):
    """Import the GSMA TAC DB data found in INPUT into the PostgreSQL database."""
    params = _common_import_params(ctx)
    gsma_tc = config.gsma_threshold_config
    params.update({'perform_historic_check': not disable_historic_check,
                   'import_size_variation_percent': gsma_tc.import_size_variation_percent,
                   'import_size_variation_absolute': gsma_tc.import_size_variation_absolute})
    with importer_factory.make_data_importer('gsma_tac', input_file, config, statsd, conn, metadata_conn,
                                             run_id, metrics_root, metrics_run_root, **params) as importer:
        importer.import_data()


@cli.command(name='stolen_list')
@click.argument('input_file', type=click.Path(exists=True, dir_okay=False), callback=_validate_input_file_extension)
@disable_historic_check_option
@click.pass_context
@common.unhandled_exception_handler
@add_delta_options
@handle_import_check_exception
@common.cli_wrapper(command='dirbs-import', subcommand='stolen_list', required_role='dirbs_core_import_stolen_list')
def stolen_list(ctx, config, statsd, logger, run_id, conn, metadata_conn, command, metrics_root, metrics_run_root,
                input_file, disable_historic_check, delta, disable_delta_adds_check, disable_delta_removes_check,
                disable_delta_updates_check):
    """Import the Stolen List data found in INPUT into the PostgreSQL database."""
    params = _common_import_params(ctx)
    st_tc = config.stolen_threshold_config
    params.update({'perform_historic_check': not disable_historic_check,
                   'delta': delta,
                   'perform_delta_adds_check': not disable_delta_adds_check,
                   'perform_delta_removes_check': not disable_delta_removes_check,
                   'import_size_variation_percent': st_tc.import_size_variation_percent,
                   'import_size_variation_absolute': st_tc.import_size_variation_absolute,
                   'perform_delta_updates_check': not disable_delta_updates_check})
    with importer_factory.make_data_importer('stolen_list', input_file, config, statsd, conn, metadata_conn,
                                             run_id, metrics_root, metrics_run_root, **params) as importer:
        importer.import_data()


@cli.command(name='pairing_list')
@click.argument('input_file', type=click.Path(exists=True, dir_okay=False), callback=_validate_input_file_extension)
@disable_historic_check_option
@click.pass_context
@common.unhandled_exception_handler
@add_delta_options
@handle_import_check_exception
@common.cli_wrapper(command='dirbs-import', subcommand='pairing_list', required_role='dirbs_core_import_pairing_list')
def pairing_list(ctx, config, statsd, logger, run_id, conn, metadata_conn, command, metrics_root, metrics_run_root,
                 input_file, disable_historic_check, delta, disable_delta_adds_check, disable_delta_removes_check,
                 disable_delta_updates_check):
    """Import the Pairing List data found in INPUT into the PostgreSQL database."""
    params = _common_import_params(ctx)
    pair_tc = config.pairing_threshold_config
    params.update({'perform_historic_check': not disable_historic_check,
                   'delta': delta,
                   'perform_delta_adds_check': not disable_delta_adds_check,
                   'perform_delta_removes_check': not disable_delta_removes_check,
                   'import_size_variation_percent': pair_tc.import_size_variation_percent,
                   'import_size_variation_absolute': pair_tc.import_size_variation_absolute,
                   'perform_delta_updates_check': not disable_delta_updates_check})
    with importer_factory.make_data_importer('pairing_list', input_file, config, statsd, conn, metadata_conn,
                                             run_id, metrics_root, metrics_run_root, **params) as importer:
        importer.import_data()


@cli.command(name='registration_list')
@click.argument('input_file', type=click.Path(exists=True, dir_okay=False), callback=_validate_input_file_extension)
@disable_historic_check_option
@click.pass_context
@common.unhandled_exception_handler
@add_delta_options
@handle_import_check_exception
@common.cli_wrapper(command='dirbs-import', subcommand='registration_list',
                    required_role='dirbs_core_import_registration_list')
def registration_list(ctx, config, statsd, logger, run_id, conn, metadata_conn, command, metrics_root,
                      metrics_run_root, input_file, disable_historic_check, delta, disable_delta_adds_check,
                      disable_delta_removes_check, disable_delta_updates_check):
    """Import the Registration list data found in INPUT into the PostgreSQL database."""
    params = _common_import_params(ctx)
    reg_tc = config.import_threshold_config
    params.update({'perform_historic_check': not disable_historic_check,
                   'delta': delta,
                   'perform_delta_adds_check': not disable_delta_adds_check,
                   'perform_delta_removes_check': not disable_delta_removes_check,
                   'import_size_variation_percent': reg_tc.import_size_variation_percent,
                   'import_size_variation_absolute': reg_tc.import_size_variation_absolute,
                   'perform_delta_updates_check': not disable_delta_updates_check})
    with importer_factory.make_data_importer('registration_list', input_file, config, statsd, conn, metadata_conn,
                                             run_id, metrics_root, metrics_run_root, **params) as importer:
        importer.import_data()


@cli.command(name='golden_list')
@click.argument('input_file', type=click.Path(exists=True, dir_okay=False), callback=_validate_input_file_extension)
@disable_historic_check_option
@click.option('--pre-hashed',
              default=False,
              help='DANGEROUS: The input file contains normalized IMEIs that have already been hashed using the '
                   'MD5 algorithm. If IMEIs have not been normalized or hashed according to DIRBS Core rules, the '
                   'IMEIs in the imported list may not be correctly excluded from being blocked.')
@click.pass_context
@common.unhandled_exception_handler
@add_delta_options
@handle_import_check_exception
@common.cli_wrapper(command='dirbs-import', subcommand='golden_list', required_role='dirbs_core_import_golden_list')
def golden_list(ctx, config, statsd, logger, run_id, conn, metadata_conn, command, metrics_root, metrics_run_root,
                input_file, pre_hashed, disable_historic_check, delta, disable_delta_adds_check,
                disable_delta_removes_check, disable_delta_updates_check):
    """
    Import the Golden list data found in INPUT into the PostgreSQL database.

    NOTE: Use caution when adding entries to the Golden list, as any IMEIs added to this list will never be blocked.
    """
    params = _common_import_params(ctx)
    golden_tc = config.golden_threshold_config
    params.update({'perform_historic_check': not disable_historic_check,
                   'prehashed_input_data': pre_hashed,
                   'delta': delta,
                   'perform_delta_adds_check': not disable_delta_adds_check,
                   'perform_delta_removes_check': not disable_delta_removes_check,
                   'import_size_variation_percent': golden_tc.import_size_variation_percent,
                   'import_size_variation_absolute': golden_tc.import_size_variation_absolute,
                   'perform_delta_updates_check': not disable_delta_updates_check})
    with importer_factory.make_data_importer('golden_list', input_file, config, statsd, conn, metadata_conn,
                                             run_id, metrics_root, metrics_run_root, **params) as importer:
        importer.import_data()


@cli.command(name='barred_list')
@click.argument('input_file', type=click.Path(exists=True, dir_okay=False), callback=_validate_input_file_extension)
@disable_historic_check_option
@click.pass_context
@common.unhandled_exception_handler
@add_delta_options
@handle_import_check_exception
@common.cli_wrapper(command='dirbs-import', subcommand='barred_list', required_role='dirbs_core_import_barred_list')
def barred_list(ctx, config, statsd, logger, run_id, conn, metadata_conn, command, metrics_root, metrics_run_root,
                input_file, disable_historic_check, delta, disable_delta_adds_check, disable_delta_removes_check,
                disable_delta_updates_check):
    """Import the Barred List data found in INPUT into the PostgreSQL database."""
    params = _common_import_params(ctx)
    bl_tc = config.barred_threshold_config
    params.update({'perform_historic_check': not disable_historic_check,
                   'delta': delta,
                   'perform_delta_adds_check': not disable_delta_adds_check,
                   'perform_delta_removes_check': not disable_delta_removes_check,
                   'import_size_variation_percent': bl_tc.import_size_variation_percent,
                   'import_size_variation_absolute': bl_tc.import_size_variation_absolute,
                   'perform_delta_updates_check': not disable_delta_updates_check})
    with importer_factory.make_data_importer('barred_list', input_file, config, statsd, conn, metadata_conn,
                                             run_id, metrics_root, metrics_run_root, **params) as importer:
        importer.import_data()


@cli.command(name='barred_tac_list')
@click.argument('input_file', type=click.Path(exists=True, dir_okay=False), callback=_validate_input_file_extension)
@disable_historic_check_option
@click.pass_context
@common.unhandled_exception_handler
@add_delta_options
@handle_import_check_exception
@common.cli_wrapper(command='dirbs-import', subcommand='barred_tac_list',
                    required_role='dirbs_core_import_barred_list')
def barred_tac_list(ctx, config, statsd, logger, run_id, conn, metadata_conn, command, metrics_root, metrics_run_root,
                    input_file, disable_historic_check, delta, disable_delta_adds_check, disable_delta_removes_check,
                    disable_delta_updates_check):
    """Import the Barred TAC List data found in INPUT into the PostgreSQL database."""
    params = _common_import_params(ctx)
    btl_tc = config.barred_tac_threshold_config
    params.update({'perform_historic_check': not disable_historic_check,
                   'delta': delta,
                   'perform_delta_adds_check': not disable_delta_adds_check,
                   'perform_delta_removes_check': not disable_delta_removes_check,
                   'import_size_variation_percent': btl_tc.import_size_variation_percent,
                   'import_size_variation_absolute': btl_tc.import_size_variation_absolute,
                   'perform_delta_updates_check': not disable_delta_updates_check})
    with importer_factory.make_data_importer('barred_tac_list', input_file, config, statsd, conn, metadata_conn,
                                             run_id, metrics_root, metrics_run_root, **params) as importer:
        importer.import_data()


@cli.command(name='subscribers_registration_list')
@click.argument('input_file', type=click.Path(exists=True, dir_okay=False), callback=_validate_input_file_extension)
@disable_historic_check_option
@click.pass_context
@common.unhandled_exception_handler
@add_delta_options
@handle_import_check_exception
@common.cli_wrapper(command='dirbs-import',
                    subcommand='subscribers_registration_list',
                    required_role='dirbs_core_import_subscribers_registration_list')
def subscribers_registration_list(ctx, config, statsd, logger, run_id, conn, metadata_conn, command,
                                  metrics_root, metrics_run_root, input_file, disable_historic_check,
                                  delta, disable_delta_adds_check, disable_delta_removes_check,
                                  disable_delta_updates_check):
    """Import the Subscribers Registration List data found in INPUT into the PostgreSQL database."""
    params = _common_import_params(ctx)
    subscribers_tc = config.subscribers_threshold_config
    params.update({'perform_historic_check': not disable_historic_check,
                   'delta': delta,
                   'perform_delta_adds_check': not disable_delta_adds_check,
                   'perform_delta_removes_check': not disable_delta_removes_check,
                   'import_size_variation_percent': subscribers_tc.import_size_variation_percent,
                   'import_size_variation_absolute': subscribers_tc.import_size_variation_absolute,
                   'perform_delta_updates_check': not disable_delta_updates_check})
    with importer_factory.make_data_importer('subscribers_registration_list', input_file, config, statsd, conn,
                                             metadata_conn, run_id, metrics_root,
                                             metrics_run_root, **params) as importer:
        importer.import_data()


@cli.command(name='device_association_list')
@click.argument('input_file', type=click.Path(exists=True, dir_okay=False), callback=_validate_input_file_extension)
@disable_historic_check_option
@click.pass_context
@common.unhandled_exception_handler
@add_delta_options
@handle_import_check_exception
@common.cli_wrapper(command='dirbs-import',
                    subcommand='device_association_list',
                    required_role='dirbs_core_import_device_association_list')
def device_association_list(ctx, config, statsd, logger, run_id, conn, metadata_conn, command,
                            metrics_root, metrics_run_root, input_file, disable_historic_check,
                            delta, disable_delta_adds_check, disable_delta_removes_check,
                            disable_delta_updates_check):
    """Import the Device Association List data found in INPUT file."""
    params = _common_import_params(ctx)
    associations_tc = config.associations_threshold_config
    params.update({'perform_historic_check': not disable_historic_check,
                   'delta': delta,
                   'perform_delta_adds_check': not disable_delta_adds_check,
                   'perform_delta_removes_check': not disable_delta_removes_check,
                   'import_size_variation_percent': associations_tc.import_size_variation_percent,
                   'import_size_variation_absolute': associations_tc.import_size_variation_absolute,
                   'perform_delta_updates_check': not disable_delta_updates_check})

    with importer_factory.make_data_importer('device_association_list', input_file, config, statsd, conn,
                                             metadata_conn, run_id, metrics_root, metrics_run_root,
                                             **params) as importer:
        importer.import_data()


@cli.command(name='monitoring_list')
@click.argument('input_file', type=click.Path(exists=True, dir_okay=False), callback=_validate_input_file_extension)
@disable_historic_check_option
@click.pass_context
@common.unhandled_exception_handler
@add_delta_options
@handle_import_check_exception
@common.cli_wrapper(command='dirbs-import',
                    subcommand='monitoring_list',
                    required_role='dirbs_core_import_monitoring_list')
def monitoring_list(ctx, config, statsd, logger, run_id, conn, metadata_conn, command, metrics_root, metrics_run_root,
                    input_file, disable_historic_check, delta, disable_delta_adds_check, disable_delta_removes_check,
                    disable_delta_updates_check):
    """Import the monitoring list data found in INPUT file."""
    params = _common_import_params(ctx)
    ml_tc = config.monitoring_threshold_config
    params.update({'perform_historic_check': not disable_historic_check,
                   'delta': delta,
                   'perform_delta_adds_check': not disable_delta_adds_check,
                   'perform_delta_removes_check': not disable_delta_removes_check,
                   'import_size_variation_percent': ml_tc.import_size_variation_percent,
                   'import_size_variation_absolute': ml_tc.import_size_variation_absolute,
                   'perform_delta_updates_check': not disable_delta_updates_check})
    with importer_factory.make_data_importer('monitoring_list', input_file, config, statsd, conn, metadata_conn,
                                             run_id, metrics_root, metrics_run_root, **params) as importer:
        importer.import_data()
