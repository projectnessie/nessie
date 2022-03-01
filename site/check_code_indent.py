#  Copyright (C) 2022 Dremio
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
import re
import sys
from pathlib import Path

if __name__ == '__main__':
  found_errors = False
  re_code_snippet = re.compile("^[ ]*```")
  for p in Path('.').glob('**/*.md'):
    opening_block = None
    with open(p) as f:
      line_no = 0
      for line in f.readlines():
        line_no += 1
        match = re_code_snippet.match(line)
        if not match:
          continue
        block = match.group(0)
        if opening_block is None:
          opening_block = block
        else:
          if len(block) != len(opening_block):
            print(f"Code block ending with wrong indentation {p}:{line_no}")
            found_errors = True
          opening_block = None
  if found_errors:
    print("Some code blocks had an invalid indentation!")
    sys.exit(1)
  print("All code blocks had the right indentation!")
