#!/bin/sh
#
# Makefile for importing barred-tac list data.
#
# Copyright (c) 2018-2021 Qualcomm Technologies, Inc.
#
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without modification, are permitted (subject to the
# limitations in the disclaimer below) provided that the following conditions are met:
#
# - Redistributions of source code must retain the above copyright notice, this list of conditions and the following
#   disclaimer.
# - Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following
#   disclaimer in the documentation and/or other materials provided with the distribution.
# - Neither the name of Qualcomm Technologies, Inc. nor the names of its contributors may be used to endorse or promote
#   products derived from this software without specific prior written permission.
# - The origin of this software must not be misrepresented; you must not claim that you wrote the original software.
#   If you use this software in a product, an acknowledgment is required by displaying the trademark/logo as per the
#   details provided here: https://www.qualcomm.com/documents/dirbs-logo-and-brand-guidelines
# - Altered source versions must be plainly marked as such, and must not be misrepresented as being the original software.
# - This notice may not be removed or altered from any source distribution.
#
# NO EXPRESS OR IMPLIED LICENSES TO ANY PARTY'S PATENT RIGHTS ARE GRANTED BY THIS LICENSE. THIS SOFTWARE IS PROVIDED BY
# THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
# THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
# COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
# BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.
#
#

BARRED_TAC_LIST_HOME = .

# Use COMMON_IMPORTER_ARGS to pass in general args valid for dirbs-import
COMMON_OPERATOR_ARGS =
# Use IMPORTER_ARGS to pass in extra args specific to pairing_list type of dirbs-import
IMPORTER_ARGS =

# Global variables
LOCK_DIR = $(BARRED_TAC_LIST_HOME)/lock
LOCK_FILE = pid
PID = $(shell echo $$PPID)
CURRENT_TIME = $(shell eval date "+%Y.%m.%d-%H.%M.%S")
PROCESSED_FILE_NAME = .processed

BARRED_TAC_LIST_FILES =  $(shell find $(BARRED_TAC_LIST_HOME) -print 2>/dev/null | grep -i '$(BARRED_TAC_LIST_HOME).*.zip';)
BARRED_TAC_LIST_FILENAMES = $(notdir $(BARRED_TAC_LIST_FILES))
BARRED_TAC_LIST_PROCESSED_FILES_BASE := $(basename $(BARRED_TAC_LIST_FILENAMES))
BARRED_TAC_LIST_PROCESSED_FILES = $(addprefix $(BARRED_TAC_LIST_HOME)/, $(BARRED_TAC_LIST_PROCESSED_FILES_BASE:=$(PROCESSED_FILE_NAME)))


# Always run
.PHONY: lock unlock all

all: lock $(BARRED_TAC_LIST_PROCESSED_FILES) unlock

# Tries to make lock directory, if it can puts PID in a file within.
# Else checks to see if the process that made the lock directory is still running.
# If not it replaces the PID with its own.
# Otherwise exits
# This is a best effort lock it is possible for a process to jump in between checking the PID and writting the file.
lock:
	@if mkdir $(LOCK_DIR); \
	then \
		echo $(PID) > $(LOCK_DIR)/$(LOCK_FILE); \
	else \
		if kill -0 `cat $(LOCK_DIR)/$(LOCK_FILE)` 2> /dev/null;  \
		then \
			exit 3;\
		else \
			echo $(PID) > $(LOCK_DIR)/$(LOCK_FILE); \
		fi;\
	fi;

# Removes Lock directory
unlock:
	@rm -rf $(LOCK_DIR)

# imports the pairing list file.
%.processed: %.zip
	dirbs-import $(COMMON_IMPORTER_ARGS) barred_tac_list $(IMPORTER_ARGS) $<;
	touch $@;
