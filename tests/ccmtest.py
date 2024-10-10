# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import os
from unittest import TestCase

try:
    FileNotFoundError
except NameError:
    FileNotFoundError = IOError  # Python 2.7 compatibility

class Tester(TestCase):

    check_log_errors = True

    def __init__(self, *argv, **kwargs):
        super(Tester, self).__init__(*argv, **kwargs)

    def setUp(self):
        self.check_log_errors = True

    def tearDown(self):
        if hasattr(self, 'cluster'):
            try:
                if self.check_log_errors:
                    for node in self.cluster.nodelist():
                        try:
                            self.assertListEqual(node.grep_log_for_errors(), [])
                        except FileNotFoundError:
                            continue
            finally:
                test_path = self.cluster.get_path()
                self.cluster.remove()
                if os.path.exists(test_path):
                    os.remove(test_path)
