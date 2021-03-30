"""
DIRBS CLI for list generation (Blacklist, Exception, Notification). Installed as a dirbs-listgen console script.

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

import click

import dirbs.cli.common as common
from dirbs.listgen import ListsGenerator


@click.command()
@common.setup_initial_logging
@click.version_option()
@common.parse_verbosity_option
@common.parse_db_options
@common.parse_statsd_options
@common.parse_multiprocessing_options
@click.option('--curr-date',
              help='DANGEROUS: Sets current date in YYYYMMDD format for testing. By default, uses '
                   'system current date.',
              callback=common.validate_date)
@click.option('--no-full-lists',
              is_flag=True,
              help='If set, disable outputting full lists as CSV for a performance improvement.')
@click.option('--no-cleanup',
              is_flag=True,
              help='If set, intermediate tables used to calculate lists will not be deleted so that they can be '
                   'inspected.')
@click.option('--base', type=int, default=-1, help='If set, will use this run ID as the base for the delta CSV lists.')
@click.option('--disable-sanity-checks', is_flag=True,
              help='If set sanity checks on list generation will be disabled (might cause large delta generation)')
@click.option('--conditions',
              help='By default, dirbs-listgen generates lists for all blocking conditions. Specify a comma-separated '
                   'list of blocking condition names if you wish to generate lists only for those conditions. The '
                   'condition name corresponds to the label parameter of the condition in the DIRBS configuration.',
              callback=common.validate_blocking_conditions,
              default=None)
@click.argument('output_dir',
                type=click.Path(exists=True, file_okay=False, writable=True))
@click.pass_context
@common.unhandled_exception_handler
@common.configure_logging
@common.cli_wrapper(command='dirbs-listgen', required_role='dirbs_core_listgen')
def cli(ctx, config, statsd, logger, run_id, conn, metadata_conn, command, metrics_root, metrics_run_root,
        curr_date, no_full_lists, no_cleanup, base, disable_sanity_checks, output_dir, conditions):
    """DIRBS script to output CSV lists (blacklist, exception, notification) for the current classification state."""
    if curr_date is not None:
        logger.warning('*************************************************************************')
        logger.warning('WARNING: --curr-date option passed to dirbs-listgen')
        logger.warning('*************************************************************************')
        logger.warning('')
        logger.warning('This should not be done in a production DIRBS deployment for the following reasons:')
        logger.warning('')
        logger.warning('1. Current date determines which of the blacklist or the notifications list a classified')
        logger.warning('   IMEI ends up on. If --curr-date is set to a date in the future, it is possible that ')
        logger.warning('   classified IMEIs might erroneously end up on the blacklist before their grace period has')
        logger.warning('   expired. If set to the past, blacklisted IMEIs will potentially be considered to be')
        logger.warning('   in their grace period again and be re-notified.')
        logger.warning('2. Because changing the current date can affect whether IMEIs are on the blacklist vs.')
        logger.warning('   the notifications lists, this can produce large, invalid delta files in the lists.')
        logger.warning('')

    list_generator = ListsGenerator(config=config, logger=logger, run_id=run_id, conn=conn,
                                    metadata_conn=metadata_conn, curr_date=curr_date, no_full_lists=no_full_lists,
                                    no_cleanup=no_cleanup, base_run_id=base, conditions=conditions,
                                    disable_sanity_checks=disable_sanity_checks, output_dir=output_dir)
    list_generator.generate_lists()
