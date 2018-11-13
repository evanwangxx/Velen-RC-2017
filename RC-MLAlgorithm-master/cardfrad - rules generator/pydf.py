# -*- coding: utf-8 -*-

import collections
import json


class AttrDict(dict):
   def __getitem__(self, key):
       return self.get(key)

   def __getattr__(self, attr):
       return self.get(attr)

   def __setattr__(self, attr, value):
       self[attr] = value


class OrderedSet(collections.Set):
   def __init__(self, *iterables):
       self.elements = []
       for iterable in iterables:
           for value in iterable:
               if value not in self.elements:
                   self.elements.append(value)

   def __iter__(self):
       return iter(self.elements)

   def __str__(self):
       return str(self.elements)

   def __contains__(self, value):
       return value in self.elements

   def __len__(self):
       return len(self.elements)


class Feats(AttrDict):
   PARAMS = []
   FETCHERS = []
   FIELD = ''
   PREFIX = ''

   def __init__(self):
       self.context__ = AttrDict()

   @classmethod
   def fetcher(cls, *names, **kwargs):
       def _wrap(function):
           for name in names:
               if name in cls.PARAMS:
                   raise Exception('duplicate parameter %s' % name)
               cls.PARAMS.append(name)

           cls.FETCHERS.append((names, function))
           def __wrap(*args, **kwargs):
               raise Exception('feature can not call')

           return __wrap

       return _wrap

   @classmethod
   def register_feats(cls):
       pass

   def _str(self, v):
       if v is None:
           return ''
       elif isinstance(v, str) or isinstance(v, unicode):
           return v 

       return str(v)

   def tolist(self, columns=None):
       if columns is None:
           columns = self.PARAMS

       return map(lambda k: self._str(self[k]), columns)

   def tojson(self, columns=None):
       if columns is None:
           columns = self.PARAMS

       data = dict(map(lambda k: (k, self[k]), columns))
       return json.dumps(data)

   def calculate(self, d, dd):
       for names, function in self.FETCHERS:
           values = function(d, self, self.context__, dd)
           if not isinstance(values, tuple):
               values = (values, )

           assert len(names) == len(values), 'values not match params'
           for name, value in zip(names, values):
               self[name] = value


class DataFrame(object):
   def __init__(self, data, index=None, columns=None):
       assert isinstance(data, 
                   collections.Iterable), 'data must be Iterable'
       data = list(map(lambda d: list(d), data))
       
       if index is None:
           index = range(len(data))
   
       assert isinstance(index, 
                   collections.Iterable), 'index must be Iterable or None'
       index = list(index)
           
       if columns is None:
           columns = range(len(data[0])) if data else ()

       self._d = data
       self._i = index
       self._c = columns

       self._ilocer = ILoc(self)
       self._locer = Loc(self)

       self._fix()

   def _fix(self):
       for c in self.columns:
           try:
               self[c] = self[c].astype(float)
           except:
               pass  

   @property
   def columns(self):
       return self._c

   @columns.setter
   def columns(self, cols):
       assert len(cols) == len(self._c), 'columns num not match'

       self._c = tuple(cols) 

   @property
   def index(self):
       return self._i

   @columns.setter
   def index(self, idx):
       assert len(idx) == len(self._i), 'index num not match'

       self._i = tuple(idx)     

   @property
   def empty(self):
       return self.shape[0] == 0

   @property
   def shape(self):
       return len(self._d), len(self._c)

   @staticmethod
   def _filter_key(key):
       i_key = enumerate(key)
       i_key_filtered = filter(lambda a: a[1] is not False, i_key)
       def _f(a):
           if a[1] is True:
               return a[0]
           elif isinstance(a[1], int):
               return a[1]

       return map(_f, i_key_filtered)

   def __getitem__(self, key):

       if isinstance(key, Series):
           key = key._d 

           key_ = DataFrame._filter_key(key)
           data = map(lambda i: self._d[i], key_)
           index = map(lambda i: self._i[i], key_)
           return DataFrame(data, index=index, columns=self._c)
       
       if isinstance(key, tuple) or isinstance(key, list):
           cis = map(lambda c: self._c.index(c), key)
           data = map(lambda d: [d[c] for c in cis], self._d)
           return DataFrame(data, index=self._i, columns=key)

       if isinstance(key, slice):
           data = self._d[key]
           index = self._i[key]
           return DataFrame(data, index=index, columns=self._c)

       if key in self._c:
           ci = self._c.index(key)
           return Series(map(lambda d: d[ci], self._d), 
                                       index=self._i, column=key)

       raise Exception('DataFrame key error: %s' % key)

   def __setitem__(self, key, value):
       if key in self._c:
           if isinstance(value, Series):
               value = value._d

           ci = self._c.index(key)
           for i in range(self.shape[0]):
               self._d[i][ci] = value[i]
           return

       raise Exception('key not in columns')

   def _iloc(self, key):
       if isinstance(key, slice):
           return self[key]

       return Row(self._d[key], self._c)

   @property
   def iloc(self):
       return self._ilocer

   def _loc(self, key):
       i = self._i.index(key)

       return Row(self._d[i], self._c)

   @property
   def loc(self):
       return self._locer
           

   def iterrows(self):
       for i, d in zip(self._i, self._d):
           yield i, Row(d, self.columns)

   def itercols(self):
       for col in self.columns:
           yield col, self[col]

   def apply(self, func, axis=1, **kwargs):
       if axis == 1:
           res = map(lambda r: func(r[1], **kwargs), self.iterrows())
       else:
           res = map(lambda c: func(c[1], **kwargs), self.itercols())

       return DataFrame(res)        

   def sort(self, sorts=None, ascending=True):
       if not sorts:
           sorts = self.columns

       if isinstance(sorts, str) or not isinstance(sorts, collections.Iterable):
           sorts = (sorts, )

       cis = map(lambda c: self._c.index(c), sorts)
       reverse = not ascending
       order = Series(map(lambda a: a[0], sorted(enumerate(self._d), 
                   key=lambda x: map(lambda i: x[1][i], cis), reverse=reverse)))

       return self[order]       

   def groupby(self, col):
       data = self.sort(col)
       _col = data[col].value
       groups, index = [], []
       start = 0
       for i in range(1, len(_col)):
           if _col[i] == _col[i-1]:
               pass
           else:
               index.append(_col[start])
               groups.append(data[Series(range(start, i))])
               start = i

           if i == len(_col) - 1:
               index.append(_col[start])
               groups.append(data[Series(range(start, i+1))])
               
       return GroupData(groups, index)

   def drop_duplicates(self, col=None, sorts=None):
       if col is None:
           return self

       if self.empty:
           return self

       st = self[col].argsort()

       arr = self[st]

       tmp = arr[col]
       tmp1 = tmp.roll(1)

       diff = tmp != tmp1

       diff.iloc[0] = True
       return arr[diff]

   def __str__(self):
       i_str = ' \t%s' % ('\t'.join(map(str, self.columns)))
       d_str = '\n'.join(map(lambda d: '%s\t%s' % (d[0], '\t'.join(map(str, d[1]))), 
                               zip(self._i, self._d)))
       return '%s\n%s' % (i_str, d_str)
   __repr__ = __str__

   def __getattr__(self, key):
       if key in self._c:
           return self[key]

       raise AttributeError('DataFrame object has no attribute %s' % key)

   def to_csv(self, path, delimeter=',', index=False):
       f = open(path, 'wt+')
       
       if index:
           f.write('id%s%s\n' % (delimeter, delimeter.join(map(str, self.columns))))
       else:
           f.write('%s\n' % delimeter.join(map(str, self.columns)))

       for i in range(self.shape[0]):
           if index:
               line = '%s%s%s\n' % (self.index[i], delimeter, 
                                       delimeter.join(map(str, self._d[i])))
           else:
               line = '%s\n' % delimeter.join(map(str, self._d[i]))
           f.write(line)

       f.close()


class GroupData(object):
   def __init__(self, data, index):
       self._d = list(data)
       self.index = list(index)

   def _append(self, index, data):
       self._d.append(data)
       self.index.append(index)

   def apply(self, func):
       res = []
       for d in self._d:
           res.append(Series(func(d)))

       return DataFrame(res, self.index)

   def filter(self, func):
       data, index = [], []
       for i, d in zip(self.index, self._d):
           if func(d):
               data.append(d)
               index.append(i)
       return GroupData(data, index)

   def flat(self):
       if len(self._d) == 0:
           data = []
           index = self.index
           columns = []
       elif len(self._d) == 1:
           data = self._d[0]
           index = self.index
           columns = self._d[0].columns
       else:
           data = np.concatenate(map(lambda d: d._d, self._d), axis=0)
           index = []
           for i, d in zip(self.index, self._d):
               index += [i for _ in range(d.shape[0])]
           columns = self._d[0].columns

       return DataFrame(data, index=index, columns=columns)

   def __str__(self):
       return '\n'.join(map(lambda d: '%s: \n%s' % d, zip(self.index, self._d)))


class Row(dict):
   def __init__(self, values, keys=None):
       super(Row, self).__init__()

       if not keys:
           keys = range(len(values))
       self['_keys'] = keys
       for k, v in zip(keys, values):
           self[k] = v

       self.__dict__ = self

   def __iter__(self):
       for k in self._keys:
           yield self[k]

   def tojson(self):
       od = collections.OrderedDict()
       for k in self._keys:
           od[k] = self[k]

       return json.dumps(od)

   def tolist(self):
       def _str(v):
           if isinstance(v, str) or isinstance(v, unicode):
               return v 
           return str(v)

       return [_str(self[k]) for k in self._keys]


class ILoc(object):
   def __init__(self, target):
       self.target = target

   def __getitem__(self, key):
       return self.target._iloc(key)

   def __setitem__(self, key, value):
       return self.target._set_iloc_item(key, value)


class Loc(object):
   def __init__(self, target):
       self.target = target

   def __getitem__(self, key):
       return self.target._loc(key)

   def __setitem__(self, key, value):
       return self.target._set_loc_item(key, value)


class Series(object):
   def __init__(self, data, index=None, column=None):
       assert isinstance(data, collections.Iterable), 'data must be Iterable'
       data = list(data)
       
       if index is None:
           index = range(len(data))

       assert isinstance(index, 
                   collections.Iterable), 'index must be Iterable or None'
       index = list(index)
           
       if column is None:
           column = '1'

       self._d = data
       self._i = index
       self._c = column

       self._ilocer = ILoc(self)

   @property
   def column(self):
       return self._c

   @staticmethod
   def _filter_key(key):
       i_key = enumerate(key)
       i_key_filtered = filter(lambda a: a[1] is not False, i_key)
       def _f(a):
           if a[1] is True:
               return a[0]
           elif isinstance(a[1], int):
               return a[1]

       return map(_f, i_key_filtered)

   def __getitem__(self, key):
       if isinstance(key, Series):
           key = key._d 
       
       assert not(key is True or key is False), 'Series key must not be bool'

       if isinstance(key, slice):
           return Series(self._d[key], self._i[key], self._c)

       if key in self._i:
           return self._d[self._i.index(key)]

       key_ = Series._filter_key(key)
       data = map(lambda i: self._d[i], key_)
       index = map(lambda i: self._i[i], key_)
       
       return Series(data, index, self._c)

   def _iloc(self, key):
       return self._d[key]

   @property
   def iloc(self):
       return self._ilocer

   def _set_iloc_item(self, key, value):
       self._d[key] = value

   def __setitem__(self, key, value):
       assert key in self._i, 'key %s not in index' % key
       
       self._d[self._i.index(key)] = value

   # def __getattr__(self, key):
   #     assert key in self._i, 'key %s not in index' % key
       
   #     return self[key]

   def sum(self):
       data = self[~self.isnull()]._d
       return sum(data)

   def mean(self):
       data = self[~self.isnull()]._d
       if not data:
           return 0
       return sum(data) / float(len(data))

   def median(self):
       data = self[~self.isnull()]._d
       if not data:
           return 0

       sorts = sorted(data)
       length = len(sorts)
       if not length % 2:
           return (sorts[length / 2] + sorts[length / 2 - 1]) / 2.0
       return sorts[length / 2]

   def max(self):
       data = self[~self.isnull()]._d
       return max(data)

   def count(self):
       return len(self._d)

   def value_counts(self):
       _cnts = dict()
       for value in self.value:
           _cnts[value] = _cnts.get(value, 0) + 1

       cnts = sorted(_cnts.items(), key=lambda a: a[1], reverse=True)
       return Series(map(lambda a: a[1], cnts), 
                   index=map(lambda a: a[0], cnts), column='count')

   def isnull(self, values=()):
       def null(a):
           if a is None or a in values:
               return True
           try:
               a = str(a)
               return a == 'nan' or a == ''
           except:
               return False

       return Series(map(null, self._d))

   def unique(self):
       return Series(set(self._d), column=self.column)

   def isin(self, other):
       assert (isinstance(other, collections.Iterable) and 
                   not isinstance(other, str) and 
                   not isinstance(other, unicode)), 'must be list or tuple'
       other = set(other)
       return Series(map(lambda a: a in other, self._d))

   @property
   def index(self):
       return self._i

   @property
   def value(self):
       return self._d

   @property
   def shape(self):
       return len(self._d), 1

   @property
   def empty(self):
       return self.shape[0] == 0
   
   def __str__(self):
       i_str = ' \t%s' % self.column
       d_str = '\n'.join(map(lambda d: '\t'.join((str(d[0]), str(d[1]))), 
                                       zip(self._i, self._d)))
       return '%s\n%s' % (i_str, d_str)
   __repr__ = __str__

   def cmp(self, other, func):
       if isinstance(other, Series):
           other = other.value

       if (isinstance(other, collections.Iterable) and not isinstance(other, str)
                   and not isinstance(other, unicode)):
           return Series(map(lambda d: func(d[0], d[1]), zip(self._d, other))
                       , index=self._i, column=self._c)

       return Series(map(lambda d: func(d, other), self._d)
                   , index=self._i, column=self._c)

   def __eq__(self, other):
       return self.cmp(other, lambda a, b: a == b)

   def __ne__(self, other):
       return self.cmp(other, lambda a, b: a != b)

   def __gt__(self, other):
       return self.cmp(other, lambda a, b: a > b)

   def __lt__(self, other):
       return self.cmp(other, lambda a, b: a < b)

   def __ge__(self, other):
       return self.cmp(other, lambda a, b: a >= b)

   def __le__(self, other):
       return self.cmp(other, lambda a, b: a <= b)

   def __len__(self):
       return len(self._d)

   def __add__(self, other):
       return self.cmp(other, lambda a, b: a + b)

   def __sub__(self, other):
       return self.cmp(other, lambda a, b: a - b)

   def __invert__(self):
       res = map(lambda a: False if a else True, self._d)
       return Series(res)

   def __and__(self, other):
       return self.cmp(other, lambda a, b: a and b)

   def __or__(self, other):
       return self.cmp(other, lambda a, b: a or b)

   def argsort(self, asc=True):
       return Series(map(lambda a: a[0], sorted(enumerate(self._d), 
                               key=lambda a: a[1], reverse=not asc)))

   def apply(self, func):
       return Series(map(func, self._d), index=self._i, column=self._c)

   def roll(self, shift=1):
       data = collections.deque(self._d)
       data.rotate(shift)
       index = collections.deque(self._i)
       index.rotate(shift)

       return Series(data, index=index, column=self._c)

   def copy(self):
       return Series(self._d, self._i, self._c)

   def __iter__(self):
       return iter(self._d)

   def astype(self, target_type, null_values=(
                   '', 'NULL', 'null', 'None', 'none', ' ', r'\t')):
       def cast(d):
           if (d is None or d in null_values):
               # return target_type()
               return None
           else:
               return target_type(d)
       data = map(cast, self._d)
       return Series(data, index=self._i, column=self._c)

   def to_dict(self):
       return dict(zip(self._i, self._d))

   def to_list(self):
       return list(self._d)


def read_csv(path, delimeter=',', header=True, index=False, **kwargs):
   assert isinstance(index, int), 'index must be integer'

   f = open(path, 'r')
   columns = map(lambda c: c.strip(), 
                   f.readline().split(delimeter)) if header else None        

   data = []
   for line in f:
       ds = map(lambda d: d.strip(), line.split(delimeter))
       data.append(ds)

   if index:
       if columns:
           columns = columns[1:]

       _index = map(lambda d: d[0], data)
       data = map(lambda d: d[1:], data)
   else:
       _index = None

   return DataFrame(data, index=_index, columns=columns)


