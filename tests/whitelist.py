"""
Whitelist unit tests.

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

from click.testing import CliRunner

from dirbs.cli.whitelist import cli as dirbs_whitelist_cli
from _fixtures import *  # noqa: F403, F401


def test_wl_not_enabled_check(mocked_config, monkeypatch):
    """Verifies that the cli can detect if whitelist is not enabled in config."""
    monkeypatch.setattr(mocked_config.operational_config, 'activate_whitelist', False)
    runner = CliRunner()

    # verify for processor cli
    result = runner.invoke(dirbs_whitelist_cli, ['process'], obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 1

    # verify for distributor cli
    result = runner.invoke(dirbs_whitelist_cli, ['distribute'], obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 1


def test_wl_restricted_distribution(mocked_config, monkeypatch):
    """Verifies that when whitelist sharing is restricted in config, the cli detects."""
    monkeypatch.setattr(mocked_config.operational_config, 'restrict_whitelist', True)
    runner = CliRunner()
    result = runner.invoke(dirbs_whitelist_cli, ['distribute'], obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 1


def test_wl_process_no_broker(mocked_config, monkeypatch, logger):
    """Verifies that if there is no broker available the command exists."""
    monkeypatch.setattr(mocked_config.operational_config, 'activate_whitelist', True)
    monkeypatch.setattr(mocked_config.broker_config.kafka, 'hostname', 'abchost')
    runner = CliRunner()
    result = runner.invoke(dirbs_whitelist_cli, ['process'], obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 1


def test_wl_distribute_no_broker(mocked_config, monkeypatch, logger):
    """Verifies that if there is no broker available the command exists."""
    monkeypatch.setattr(mocked_config.operational_config, 'activate_whitelist', True)
    monkeypatch.setattr(mocked_config.operational_config, 'restrict_whitelist', False)
    monkeypatch.setattr(mocked_config.broker_config.kafka, 'hostname', 'abchost')

    runner = CliRunner()
    result = runner.invoke(dirbs_whitelist_cli, ['distribute'], obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 1
