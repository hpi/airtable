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
            print(self.sources)
            with f.open(encoding="utf-8") as fo:
                yield json.load(fo)

    def tableRecords(self, findBaseName = None, findTableName = None) -> Iterator[Res[Json]]:
      raw = self.raw()

      for baseName in keys(raw):
        if findBaseName:
          if baseName != findBaseName:
            yield

          for table in raw['recordsByTable']:
            if findTableName:
              if table['name'] == findTableName:
                yield table
            else:
              yield table

if __name__ == '__main__':
    dal_helper.main(DAL=DAL)
