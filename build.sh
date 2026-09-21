#!/bin/bash
#############################################################################
# Copyright (c) 2023 Huawei Technologies Co.,Ltd.
#
# openGauss is licensed under Mulan PSL v2.
# You can use this software according to the terms
# and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#
#   http://license.coscl.org.cn/MulanPSL2
#
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.
# ----------------------------------------------------------------------------
# Description  : shell script for build datakit and plugins.
#############################################################################
declare mvn_target="clean install package"
declare mvn_input_args=''

#############################################################################
function print_help()
{
    echo "Usage: $0 [OPTION]
    -h|--help              show help information
    -t|--target            set the mvn build target, default is clean install package
    -a|--args              set the mvn build args additions, default is empty
example:
    sh build.sh  // build in default
    sh build.sh -a \"-Dbuild.frontend.skip=true -Dweb.build.skip=true\"  // build skip frontend
    sh build.sh -t \"clean\" -a \"-Dbuild.frontend.skip=true -Dweb.build.skip=true\"  // build only clean and skip frontend
    "
}

while [ $# -gt 0 ]; do
    case "$1" in
        -h|--help)
            print_help
            exit 1
            ;;
        -t|--target)
            if [ "$2"X = X ]; then
                echo "no given correct mvn target, such as: clean install package"
                exit 1
            fi
            mvn_target=$2
            shift 2
            ;;
        -a|--args)
            if [ "$2"X = X ]; then
                echo "no given correct mvn args, such as: -Dbuild.frontend.skip=true -Dweb.build.skip=true"
                exit 1
            fi
            mvn_input_args=$2
            shift 2
            ;;
         *)
            echo "Internal Error: option processing error: $1" 1>&2
            echo "please input right paramtenter, the following command may help you"
            echo "./build.sh --help or ./build.sh -h"
            exit 1
    esac
done
root_path=`pwd`

mvn_init_args="-Dmaven.test.skip=true"
mvn_args="$mvn_init_args -U $mvn_input_args"
build_main_pkg=openGauss-tools-datachecker-performance


echo "we got build cmd: mvn $mvn_target $mvn_args"

export JAVA_TOOL_OPTIONS="-Dfile.encoding=UTF8"
pom_version=`awk '/<version>[^<]+<\/version>/ && NR>10 {gsub(/<version>|<\/version>/,"",$1);print $1;exit;}'  pom.xml`


function prepare_java_env()
{
    echo "We no longer provide java, please makesure java(1.11*) already in PATH!"
    JAVA_VERSION=`java -version 2>&1 | awk -F '"' '/version/ {print $2}'`
    echo java version is $JAVA_VERSION
}

function prepare_maven_env()
{
    echo "We no longer provide mvn, please makesure mvn(3.8.0+) already in PATH!"
    MAVEN_VERSION=`mvn -v 2>&1 | awk '/Apache Maven / {print $3}'`
    echo maven version is $MAVEN_VERSION
}

function prepare_env()
{
    prepare_java_env
    prepare_maven_env
}

function build_main_pkg() {
    mvn ${mvn_target} ${mvn_args}
    if [ $? -ne 0 ]; then
      echo "Build gs_datacheck failed..."
      exit 1
    fi
}

# The third-party notice is a static, version-controlled file at the repo root.
# Refresh it manually when dependencies change (see gen_third_party_notice.py).
# The build just copies it into the packaging directory.
function copy_third_party_notice() {
    if [ ! -f ${root_path}/Third_Part_Open_Source_Software_Notice ]; then
        echo "Third_Part_Open_Source_Software_Notice not found in project root..."
        exit 1
    fi
    cp ${root_path}/Third_Part_Open_Source_Software_Notice \
       ${root_path}/openGauss-DataCheck-${pom_version}/Third_Part_Open_Source_Software_Notice
    if [ $? -ne 0 ]; then
        echo "Copy third-party notice failed..."
        exit 1
    fi
    echo "Copied third-party notice into openGauss-DataCheck-${pom_version}/"
}

prepare_env
build_main_pkg
copy_third_party_notice

tar -zcf $root_path/openGauss-DataCheck-${pom_version}.tar.gz ./openGauss-DataCheck-${pom_version}

echo $root_path/openGauss-DataCheck-${pom_version}.tar.gz
echo "Build openGauss-DataCheck complete ..."
