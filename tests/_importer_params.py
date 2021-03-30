"""
Reusable parameter types for importer unit tests.

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

from os import path
from datetime import datetime
import logging


class ImporterParams:
    """Base params class containing parameters common to all importers."""

    def __init__(self,
                 extract=False,
                 full_path=False,
                 log_message=None,
                 importer_name='',
                 content=None,
                 filename=None,
                 extract_dir='/tmp'):
        """Constructor."""
        self.extract = extract
        self.extract_dir = extract_dir
        self.full_path = full_path
        self.log_message = log_message
        # Importer name is a string like 'operator_data' used for filenames, etc.
        self.importer_name = importer_name
        self.filename = filename
        # Content might be None if using a real file. Otherwise it contains a buffer of data
        self.content = content

    def file(self, test_dir):
        """Returns a filename for the test file to be loaded by this importer."""
        if self.content is None:
            # FileName
            if self.full_path:
                fn = self.filename
            else:
                here = path.abspath(path.dirname(__file__))
                data_dir = path.join(here, 'unittest_data/', self.importer_name)
                fn = path.join(data_dir, self.filename)
            return fn
        else:
            # Buffer
            if self.filename is None:
                self.filename = self.importer_name
            f = open(str(test_dir.join(self.filename + '.csv')), 'w')
            f.write(self.content)
            f.seek(0)
            return f.name

    def kwparams_as_dict(self):
        """Get keyword params as dict for each importer."""
        return {
            'extract': self.extract,
            'extract_dir': self.extract_dir
        }


class GSMADataParams(ImporterParams):
    """Import params class for the GSMA TAC DB importer."""

    def __init__(self, perform_historic_check=True, import_size_variation_percent=0,
                 import_size_variation_absolute=100, *args, **kwargs):
        """Constructor."""
        super().__init__(*args, **kwargs)
        self.importer_name = 'gsma'
        self.perform_historic_check = perform_historic_check
        self.import_size_variation_percent = import_size_variation_percent
        self.import_size_variation_absolute = import_size_variation_absolute

    def kwparams_as_dict(self):
        """Overrides ImporterParams.kwparams_as_dict."""
        rv = super(GSMADataParams, self).kwparams_as_dict()
        rv.update({'import_size_variation_percent': self.import_size_variation_percent,
                   'perform_historic_check': self.perform_historic_check,
                   'import_size_variation_absolute': self.import_size_variation_absolute})
        return rv


class PairListParams(ImporterParams):
    """Import params class for the pairing list importer."""

    def __init__(self, perform_historic_check=True, import_size_variation_percent=0.95,
                 import_size_variation_absolute=1000, delta=False, perform_delta_adds_check=True,
                 perform_delta_removes_check=True, perform_delta_updates_check=True, *args, **kwargs):
        """Constructor."""
        super().__init__(*args, **kwargs)
        self.perform_historic_check = perform_historic_check
        self.importer_name = 'pairing_list'
        self.import_size_variation_percent = import_size_variation_percent
        self.import_size_variation_absolute = import_size_variation_absolute
        self.delta = delta
        self.perform_delta_adds_check = perform_delta_adds_check
        self.perform_delta_removes_check = perform_delta_removes_check
        self.perform_delta_updates_check = perform_delta_updates_check

    def kwparams_as_dict(self):
        """Overrides ImporterParams.kwparams_as_dict."""
        rv = super(PairListParams, self).kwparams_as_dict()
        rv.update({'perform_historic_check': self.perform_historic_check,
                   'import_size_variation_percent': self.import_size_variation_percent,
                   'import_size_variation_absolute': self.import_size_variation_absolute,
                   'delta': self.delta,
                   'perform_delta_adds_check': self.perform_delta_adds_check,
                   'perform_delta_removes_check': self.perform_delta_removes_check,
                   'perform_delta_updates_check': self.perform_delta_updates_check})
        return rv


class StolenListParams(ImporterParams):
    """Import params class for the stolen list importer."""

    def __init__(self, perform_historic_check=True, import_size_variation_percent=0.75,
                 import_size_variation_absolute=-1, delta=False, perform_delta_adds_check=True,
                 perform_delta_removes_check=True, perform_delta_updates_check=True, *args, **kwargs):
        """Constructor."""
        super().__init__(*args, **kwargs)
        self.perform_historic_check = perform_historic_check
        self.importer_name = 'stolen_list'
        self.import_size_variation_percent = import_size_variation_percent
        self.import_size_variation_absolute = import_size_variation_absolute
        self.delta = delta
        self.perform_delta_adds_check = perform_delta_adds_check
        self.perform_delta_removes_check = perform_delta_removes_check
        self.perform_delta_updates_check = perform_delta_updates_check

    def kwparams_as_dict(self):
        """Overrides ImporterParams.kwparams_as_dict."""
        rv = super(StolenListParams, self).kwparams_as_dict()
        rv.update({'perform_historic_check': self.perform_historic_check,
                   'import_size_variation_percent': self.import_size_variation_percent,
                   'import_size_variation_absolute': self.import_size_variation_absolute,
                   'delta': self.delta,
                   'perform_delta_adds_check': self.perform_delta_adds_check,
                   'perform_delta_removes_check': self.perform_delta_removes_check,
                   'perform_delta_updates_check': self.perform_delta_updates_check})
        return rv


class BarredListParams(ImporterParams):
    """Import params class for barred list importer."""

    def __init__(self, perform_historic_check=True, import_size_variation_percent=0.75,
                 import_size_variation_absolute=-1, delta=False, perform_delta_adds_check=True,
                 perform_delta_removes_check=True, perform_delta_updates_check=True, *args, **kwargs):
        """Constructor."""
        super().__init__(*args, **kwargs)
        self.perform_historic_check = perform_historic_check
        self.importer_name = 'barred_list'
        self.import_size_variation_percent = import_size_variation_percent
        self.import_size_variation_absolute = import_size_variation_absolute
        self.delta = delta
        self.perform_delta_adds_check = perform_delta_adds_check
        self.perform_delta_removes_check = perform_delta_removes_check
        self.perform_delta_updates_check = perform_delta_updates_check

    def kwparams_as_dict(self):
        """Overrides ImporterParams.kwparams_as_dict."""
        rv = super(BarredListParams, self).kwparams_as_dict()
        rv.update({'perform_historic_check': self.perform_historic_check,
                   'import_size_variation_percent': self.import_size_variation_percent,
                   'import_size_variation_absolute': self.import_size_variation_absolute,
                   'delta': self.delta,
                   'perform_delta_adds_check': self.perform_delta_adds_check,
                   'perform_delta_removes_check': self.perform_delta_removes_check,
                   'perform_delta_updates_check': self.perform_delta_updates_check})
        return rv


class MonitoringListParams(ImporterParams):
    """Import params class for monitoring list importer."""

    def __init__(self, perform_historic_check=True, import_size_variation_percent=0.75,
                 import_size_variation_absolute=-1, delta=False, perform_delta_adds_check=True,
                 perform_delta_removes_check=True, perform_delta_updates_check=True, *args, **kwargs):
        """Constructor."""
        super().__init__(*args, **kwargs)
        self.perform_historic_check = perform_historic_check
        self.importer_name = 'monitoring_list'
        self.import_size_variation_percent = import_size_variation_percent
        self.import_size_variation_absolute = import_size_variation_absolute
        self.delta = delta
        self.perform_delta_adds_check = perform_delta_adds_check
        self.perform_delta_removes_check = perform_delta_removes_check
        self.perform_delta_updates_check = perform_delta_updates_check

    def kwparams_as_dict(self):
        """Overrides ImporterParams.kwparams_as_dict."""
        rv = super(MonitoringListParams, self).kwparams_as_dict()
        rv.update({'perform_historic_check': self.perform_historic_check,
                   'import_size_variation_percent': self.import_size_variation_percent,
                   'import_size_variation_absolute': self.import_size_variation_absolute,
                   'delta': self.delta,
                   'perform_delta_adds_check': self.perform_delta_adds_check,
                   'perform_delta_removes_check': self.perform_delta_removes_check,
                   'perform_delta_updates_check': self.perform_delta_updates_check})
        return rv


class BarredTacListParams(ImporterParams):
    """Importer params class for barred tac list importer."""

    def __init__(self, perform_historic_check=True, import_size_variation_percent=0.75,
                 import_size_variation_absolute=-1, delta=False, perform_delta_adds_check=True,
                 perform_delta_removes_check=True, perform_delta_updates_check=True, *args, **kwargs):
        """Constructor."""
        super().__init__(*args, **kwargs)
        self.perform_historic_check = perform_historic_check
        self.importer_name = 'barred_tac_list'
        self.import_size_variation_percent = import_size_variation_percent
        self.import_size_variation_absolute = import_size_variation_absolute
        self.delta = delta
        self.perform_delta_adds_check = perform_delta_adds_check
        self.perform_delta_removes_check = perform_delta_removes_check
        self.perform_delta_updates_check = perform_delta_updates_check

    def kwparams_as_dict(self):
        """Overrides ImporterParams.kwparams_as_dict."""
        rv = super(BarredTacListParams, self).kwparams_as_dict()
        rv.update({'perform_historic_check': self.perform_historic_check,
                   'import_size_variation_percent': self.import_size_variation_percent,
                   'import_size_variation_absolute': self.import_size_variation_absolute,
                   'delta': self.delta,
                   'perform_delta_adds_check': self.perform_delta_adds_check,
                   'perform_delta_removes_check': self.perform_delta_removes_check,
                   'perform_delta_updates_check': self.perform_delta_updates_check})
        return rv


class RegistrationListParams(ImporterParams):
    """Import params class for the registration list importer."""

    def __init__(self, perform_historic_check=True, import_size_variation_percent=0.75,
                 import_size_variation_absolute=-1, delta=False, perform_delta_adds_check=True,
                 perform_delta_removes_check=True, perform_delta_updates_check=True, *args, **kwargs):
        """Constructor."""
        super().__init__(*args, **kwargs)
        self.perform_historic_check = perform_historic_check
        self.importer_name = 'registration_list'
        self.import_size_variation_percent = import_size_variation_percent
        self.import_size_variation_absolute = import_size_variation_absolute
        self.delta = delta
        self.perform_delta_adds_check = perform_delta_adds_check
        self.perform_delta_removes_check = perform_delta_removes_check
        self.perform_delta_updates_check = perform_delta_updates_check

    def kwparams_as_dict(self):
        """Overrides ImporterParams.kwparams_as_dict."""
        rv = super(RegistrationListParams, self).kwparams_as_dict()
        rv.update({'perform_historic_check': self.perform_historic_check,
                   'import_size_variation_percent': self.import_size_variation_percent,
                   'import_size_variation_absolute': self.import_size_variation_absolute,
                   'delta': self.delta,
                   'perform_delta_adds_check': self.perform_delta_adds_check,
                   'perform_delta_removes_check': self.perform_delta_removes_check,
                   'perform_delta_updates_check': self.perform_delta_updates_check})
        return rv


class GoldenListParams(ImporterParams):
    """Import params class for the golden list importer."""

    def __init__(self, perform_historic_check=True, import_size_variation_percent=0.75,
                 import_size_variation_absolute=-1, prehashed_input_data=False, delta=False,
                 perform_delta_adds_check=True, perform_delta_removes_check=True, perform_delta_updates_check=True,
                 *args, **kwargs):
        """Constructor."""
        super().__init__(*args, **kwargs)
        self.perform_historic_check = perform_historic_check
        self.importer_name = 'golden_list'
        self.import_size_variation_percent = import_size_variation_percent
        self.import_size_variation_absolute = import_size_variation_absolute
        self.prehashed_input_data = prehashed_input_data
        self.delta = delta
        self.perform_delta_adds_check = perform_delta_adds_check
        self.perform_delta_removes_check = perform_delta_removes_check
        self.perform_delta_updates_check = perform_delta_updates_check

    def kwparams_as_dict(self):
        """Overrides ImporterParams.kwparams_as_dict."""
        rv = super(GoldenListParams, self).kwparams_as_dict()
        rv.update({'perform_historic_check': self.perform_historic_check,
                   'import_size_variation_percent': self.import_size_variation_percent,
                   'import_size_variation_absolute': self.import_size_variation_absolute,
                   'prehashed_input_data': self.prehashed_input_data,
                   'delta': self.delta,
                   'perform_delta_adds_check': self.perform_delta_adds_check,
                   'perform_delta_removes_check': self.perform_delta_removes_check,
                   'perform_delta_updates_check': self.perform_delta_updates_check})
        return rv


class SubscribersListParams(ImporterParams):
    """Importer params class for Subscribers Registration List."""

    def __init__(self, perform_historic_check=True, import_size_variation_percent=0.75,
                 import_size_variation_absolute=-1, delta=False, perform_delta_adds_check=True,
                 perform_delta_removes_check=True, perform_delta_updates_check=True, *args, **kwargs):
        """Constructor."""
        super().__init__(*args, **kwargs)
        self.perform_historic_check = perform_historic_check
        self.importer_name = 'subscriber_reg_list'
        self.import_size_variation_percent = import_size_variation_percent
        self.import_size_variation_absolute = import_size_variation_absolute
        self.delta = delta
        self.perform_delta_adds_check = perform_delta_adds_check
        self.perform_delta_removes_check = perform_delta_removes_check
        self.perform_delta_updates_check = perform_delta_updates_check

    def kwparams_as_dict(self):
        """Overrides ImporterParams.kwparams_as_dict."""
        rv = super(SubscribersListParams, self).kwparams_as_dict()
        rv.update({'perform_historic_check': self.perform_historic_check,
                   'import_size_variation_percent': self.import_size_variation_percent,
                   'import_size_variation_absolute': self.import_size_variation_absolute,
                   'delta': self.delta,
                   'perform_delta_adds_check': self.perform_delta_adds_check,
                   'perform_delta_removes_check': self.perform_delta_removes_check,
                   'perform_delta_updates_check': self.perform_delta_updates_check})
        return rv


class DeviceAssociationListParams(ImporterParams):
    """Importer params class for Device Association List."""

    def __init__(self, perform_historic_check=True, import_size_variation_percent=0.75,
                 import_size_variation_absolute=-1, delta=False, perform_delta_adds_check=True,
                 perform_delta_removes_check=True, perform_delta_updates_check=True, *args, **kwargs):
        """Constructor."""
        super().__init__(*args, **kwargs)
        self.perform_historic_check = perform_historic_check
        self.importer_name = 'device_association_list'
        self.import_size_variation_percent = import_size_variation_percent
        self.import_size_variation_absolute = import_size_variation_absolute
        self.delta = delta
        self.perform_delta_adds_check = perform_delta_adds_check
        self.perform_delta_removes_check = perform_delta_removes_check
        self.perform_delta_updates_check = perform_delta_updates_check

    def kwparams_as_dict(self):
        """Overrides ImporterParams.kwparams_as_dict."""
        rv = super(DeviceAssociationListParams, self).kwparams_as_dict()
        rv.update({'perform_historic_check': self.perform_historic_check,
                   'import_size_variation_percent': self.import_size_variation_percent,
                   'import_size_variation_absolute': self.import_size_variation_absolute,
                   'delta': self.delta,
                   'perform_delta_adds_check': self.perform_delta_adds_check,
                   'perform_delta_removes_check': self.perform_delta_removes_check,
                   'perform_delta_updates_check': self.perform_delta_updates_check})
        return rv


class OperatorDataParams(ImporterParams):
    """Import params class for the operator data importer."""

    def __init__(self,
                 operator='test_operator',
                 mcc_mnc_pairs=None,
                 cc=None,
                 batch_size=100000,
                 null_imei_threshold=0.05,
                 null_imsi_threshold=0.05,
                 null_msisdn_threshold=0.05,
                 null_rat_threshold=0.05,
                 null_threshold=0.05,
                 unclean_imei_threshold=0.05,
                 unclean_imsi_threshold=0.05,
                 unclean_threshold=0.05,
                 out_of_region_imsi_threshold=0.1,
                 out_of_region_msisdn_threshold=0.1,
                 out_of_region_threshold=0.1,
                 non_home_network_threshold=0.2,
                 historic_imei_threshold=0.9,
                 historic_imsi_threshold=0.9,
                 historic_msisdn_threshold=0.9,
                 perform_msisdn_import=True,
                 perform_rat_import=False,
                 perform_file_daterange_check=True,
                 perform_leading_zero_check=True,
                 perform_null_checks=True,
                 perform_unclean_checks=True,
                 perform_region_checks=True,
                 perform_home_network_check=True,
                 perform_historic_checks=True,
                 *args,
                 **kwargs):
        """Constructor."""
        super().__init__(*args, **kwargs)
        self.importer_name = 'operator'
        self.cc = ['22'] if cc is None else cc
        self.mcc_mnc_pairs = [{'mcc': '111', 'mnc': '01'}] if mcc_mnc_pairs is None else mcc_mnc_pairs
        self.batch_size = batch_size

        if operator != operator.lower():
            logger = logging.getLogger('dirbs.import')
            logger.warning('operator_id: {0} has been changed to lower case: {1}'.format(operator, operator.lower()))
        self.operator = operator.lower()

        # NULL check thresholds
        self.null_imei_threshold = null_imei_threshold
        self.null_imsi_threshold = null_imsi_threshold
        self.null_msisdn_threshold = null_msisdn_threshold
        self.null_rat_threshold = null_rat_threshold
        self.null_threshold = null_threshold
        # Unclean data thresholds
        self.unclean_imei_threshold = unclean_imei_threshold
        self.unclean_imsi_threshold = unclean_imsi_threshold
        self.unclean_threshold = unclean_threshold
        # Regional thresholds
        self.out_of_region_imsi_threshold = out_of_region_imsi_threshold
        self.out_of_region_msisdn_threshold = out_of_region_msisdn_threshold
        self.out_of_region_threshold = out_of_region_threshold
        self.non_home_network_threshold = non_home_network_threshold
        # Historic thresholds
        self.historic_imei_threshold = historic_imei_threshold
        self.historic_imsi_threshold = historic_imsi_threshold
        self.historic_msisdn_threshold = historic_msisdn_threshold
        # Switches to disable importing of certain columns
        self.perform_msisdn_import = perform_msisdn_import
        self.perform_rat_import = perform_rat_import
        # Switches to disable groups of checks
        self.perform_file_daterange_check = perform_file_daterange_check
        self.perform_leading_zero_check = perform_leading_zero_check
        self.perform_null_checks = perform_null_checks
        self.perform_unclean_checks = perform_unclean_checks
        self.perform_region_checks = perform_region_checks
        self.perform_home_network_check = perform_home_network_check
        self.perform_historic_checks = perform_historic_checks

    def file(self, test_dir):
        """Overrides ImporterParams.file."""
        if self.content is None:
            # FileName
            if self.full_path:
                fn = self.filename
            else:
                here = path.abspath(path.dirname(__file__))
                data_dir = path.join(here, 'unittest_data/', self.importer_name)
                fn = path.join(data_dir, self.filename)
            return fn
        else:
            # Buffer
            if self.filename is None:
                date_list = []
                for line in self.content.split('\n')[1:]:
                    date_list.append(datetime.strptime(line.split(',')[0], '%Y%m%d'))
                filename = '{0}_{1}_{2}'.format(self.operator,
                                                min(date_list).strftime('%Y%m%d'),
                                                max(date_list).strftime('%Y%m%d'))
            else:
                filename = self.filename
                # test_dir type is LocalPath
            f = open(str(test_dir.join(filename + '.csv')), 'w')
            f.write(self.content)
            f.seek(0)
            return f.name

    def kwparams_as_dict(self):
        """Overrides ImporterParams.kwparams_as_dict."""
        rv = super(OperatorDataParams, self).kwparams_as_dict()
        rv.update({
            'operator_id': self.operator,
            'cc': self.cc,
            'mcc_mnc_pairs': self.mcc_mnc_pairs,
            'batch_size': self.batch_size,
            'null_imei_threshold': self.null_imei_threshold,
            'null_imsi_threshold': self.null_imsi_threshold,
            'null_msisdn_threshold': self.null_msisdn_threshold,
            'null_rat_threshold': self.null_rat_threshold,
            'null_threshold': self.null_threshold,
            'unclean_imei_threshold': self.unclean_imei_threshold,
            'unclean_imsi_threshold': self.unclean_imsi_threshold,
            'unclean_threshold': self.unclean_threshold,
            'out_of_region_imsi_threshold': self.out_of_region_imsi_threshold,
            'out_of_region_msisdn_threshold': self.out_of_region_msisdn_threshold,
            'out_of_region_threshold': self.out_of_region_threshold,
            'non_home_network_threshold': self.non_home_network_threshold,
            'historic_imei_threshold': self.historic_imei_threshold,
            'historic_imsi_threshold': self.historic_imsi_threshold,
            'historic_msisdn_threshold': self.historic_msisdn_threshold,
            'perform_msisdn_import': self.perform_msisdn_import,
            'perform_rat_import': self.perform_rat_import,
            'perform_file_daterange_check': self.perform_file_daterange_check,
            'perform_leading_zero_check': self.perform_leading_zero_check,
            'perform_null_checks': self.perform_null_checks,
            'perform_unclean_checks': self.perform_unclean_checks,
            'perform_region_checks': self.perform_region_checks,
            'perform_home_network_check': self.perform_home_network_check,
            'perform_historic_checks': self.perform_historic_checks})
        return rv
