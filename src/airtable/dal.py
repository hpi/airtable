#!/usr/bin/env python3

from pathlib import PurePath, Path
from typing import List, Dict, Union, Iterator, NamedTuple, Any, Sequence, Optional, Set
import json
from pathlib import Path
from datetime import datetime
import logging

import pytz

from .exporthelpers.dal_helper import PathIsh, Json, Res

def get_logger():
    return logging_helper.logger('airtable')

class DAL:
    def __init__(self, sources: Sequence[PathIsh]) -> None:
        self.sources = [p if isinstance(p, Path) else Path(p) for p in sources]


    def raw(self):
        for f in sorted(self.sources):
            with f.open(encoding="utf-8") as fo:
                yield json.load(fo)

    def latest(self):
      latest_file = sorted(self.sources)[-1]

      with latest_file.open(encoding="utf-8") as fo:
          return json.load(fo)

    def records(self, base_name: str = None, table_name: str = None) -> Iterator[Res[Json]]:
      for base in self.latest():
        if base_name and table_name and base_name == base['name']:
          for table in base['recordsByTable']:
            if table['name'] == table_name:
              yield table
            else:
              continue

        elif base_name and base_name == base['name']:
          yield base
        elif base_name is None and table_name is None:
          yield base

if __name__ == '__main__':
    dal_helper.main(DAL=DAL)
