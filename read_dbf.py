# coding :utf8

import sys
import csv
import struct
import datetime
import decimal
import itertools

from io import StringIO
from operator import itemgetter


def dbfreader(f):
    """Returns an iterator over records in a Xbase DBF file.
    
    The first row returned contains the field names.
    The second row contains field specs: (type, size, decimal places).
    Subsequent rows contain the data records.
    If a record is marked as deleted, it is skipped.
    
    File should be opened for binary reads.
    """
    
    numrec, lenheader = struct.unpack('<xxxxLH22x', f.read(32))
    numfields = (lenheader - 33) // 32
    
    fields = []
    for fieldno in range(numfields):
        name, typ, size, deci = struct.unpack('<11sc4xBB14x', f.read(32))
        name = name.replace(b'\0', b'')  # eliminate NULs from string
        fields.append((name, typ, size, deci))
    yield [field[0] for field in fields]
    yield [tuple(field[1:]) for field in fields]
    
    terminator = f.read(1)
    assert terminator == b'\r'
    
    fields.insert(0, ('DeletionFlag', 'C', 1, 0))
    fmt = ''.join(['%ds' % fieldinfo[2] for fieldinfo in fields])
    fmtsiz = struct.calcsize(fmt)
    for i in range(numrec):
        try:
            record = struct.unpack(fmt, f.read(fmtsiz))
        except Exception as e:
            continue
        if record[0] != b' ' and record[0] != b'0':
            
            continue  # deleted record
        result = []
        for (name, typ, size, deci), value in zip(fields, record):
            if name == 'DeletionFlag':
                continue
            if typ == "N":
                value = value.replace(b'\0', b'').lstrip()
                if value == '':
                    value = 0
                elif deci:
                    if value.count(b'.') > 1:
                        v_list = value.split(b'.')
                        value = v_list[0] + v_list[-1]
                    value = decimal.Decimal(value)
                else:
                    value = int(value)
            elif typ == 'D':
                value = value.replace(b'\x00', b'0')
                y, m, d = int(value[:4]), int(value[4:6]), int(value[6:8])
                if not y and not m and not d:
                    value = 0
                else:
                    value = datetime.date(y, m, d)
            elif typ == 'L':
                value = (value in 'YyTt' and 'T') or (value in 'NnFf' and 'F') or '?'
            elif typ == 'F':
                value = float(value)
            result.append(value)
        yield result

