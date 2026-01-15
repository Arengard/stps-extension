PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=stps
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Disable building tests and shell (speeds up build, avoids test failures)
BUILD_UNITTESTS=0
BUILD_SHELL=0
DISABLE_SANITIZER=1

export BUILD_UNITTESTS
export BUILD_SHELL
export DISABLE_SANITIZER

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile