#!/usr/bin/env bash
#
# Copyright (C) 2024 Dremio
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -e

if ! which mkcert > /dev/null ; then
  echo "Please install the 'mkcert' tool - for example using 'sudo apt install mkcert"
  exit 1
fi

cd "$(dirname $0)"

mkcert --install

rm -rf certs
mkdir certs
cd certs

mkcert nessie-nginx.localhost.localdomain localhost 127.0.0.1 ::1

echo ""
echo ""
echo "When running on macOS, please add the following line to your /etc/hosts file:"
echo "127.0.0.1 nessie-nginx.localhost.localdomain"
echo ""
