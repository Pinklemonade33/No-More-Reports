import copy
import re
import pandas as pd
import os
import pickle
from datetime import datetime, date
from operator import attrgetter
from report_objects import *
import numpy

# region Classes

# region Data File


class DataFile:
    def __init__(self, df):
        if isinstance(df, pd.DataFrame):
            self.df = df
        else:
            raise ValueError('df must be a pandas data frame')

        self.dl_date = datetime.now()
        self.date_keyword = None
        self.market_keyword = None
        self.id_type = df.columns.values

    def save(self):
        try:
            data_files = pickle.load(open('datafiles.pl', 'rb'))
        except FileNotFoundError:
            data_files = list()

        data_files = [x for x in data_files if type(x) != type(self)]
        data_files.append(self)
        pickle.dump(data_files, open('datafiles.pl', 'wb'))

    def date(self):
        if self.date_keyword is not None:
            return self.df[self.date_keyword].astype('datetime64[ns]').max()
        else:
            return self.dl_date

    def filter_markets(self, markets=None):
        if markets is not None:
            if self.market_keyword is not None:
                for market in markets:
                    self.df = self.df[self.df[self.market_keyword].str.contains(market) == True]

    def validate(self):
        if self.dl_date.date() == date.today():
            return True
        else:
            return False

    file_description = 'not provided'
    file_link = 'not provided'

# endregion

# region Rootstock

# region Locations


class INVLocationsFile(DataFile):
    def __init__(self, df):
        super().__init__(df)

    file_description = 'Items in inventory and their locations (Selected divisions, All time)'
    file_link = 'not provided'


# endregion

# region Transactions
class TransactionsFile(DataFile):
    def __init__(self, df):
        super().__init__(df)
        self.id_type = self._initialize_id_type()

    def _initialize_id_type(self):
        invlocloc = (self.df['Transaction ID'] == 'INVLOCLOC').any()
        invdivdiv = (self.df['Transaction ID'] == 'INVDIVDIV').any()
        pick = (self.df['Transaction ID'] == 'SOISS').any()
        pickreverse = (self.df['Transaction ID'] == 'SOISSR').any()
        shipped = (self.df['Transaction ID'] == 'SO Shipment').any()
        received = (self.df['Transaction ID'] == 'PORCPT TO INV').any()
        return {'invlocloc': invlocloc, 'invdivdiv': invdivdiv, 'pick': pick,
                'pickreverse': pickreverse, 'shipped': shipped, 'received': received}


class INVChangesFile(TransactionsFile):
    def __init__(self, df):
        super().__init__(df)

    file_description = 'All transactions that change total inventory quantity (Selected divisions, Selected Dates)'
    file_link = 'not provided'


class DIVTransactionsFile(TransactionsFile):
    def __init__(self, df):
        super().__init__(df)

    file_description = 'Division to division transfer transactions (All divisions, Selected dates)'
    file_link = 'not provided'


class LOCTransactionFile(TransactionsFile):
    def __init__(self, df):
        super().__init__(df)

    file_description = 'Location to location transfer transactions (Selected divisions, Selected dates)'
    file_link = 'not provided'

# endregion

# region PO Lines


class PoLineFile(DataFile):
    def __init__(self, df):
        super().__init__(df)
        self.id_type = self._initialize_id_type()
        self.market_keyword = 'Division'

    def _initialize_id_type(self):
        df = self.df.loc[self.df['Qty Remaining, PO Line'].notnull() == True]
        not_received = (df['Qty Remaining, PO Line'] > 0).all()
        received = (df['Qty Remaining, PO Line'] == 0).all()
        return {'not received': not_received, 'received': received}


class PendingPoLineFile(PoLineFile):
    def __init__(self, df):
        super().__init__(df)

    file_description = 'PO Lines not fully received (All divisions, All time (Excludes 3PL))'
    file_link = 'not provided'


class ReceivedPoLineFile(PoLineFile):
    def __init__(self, df):
        super().__init__(df)

    file_description = 'PO Lines fully received (All divisions, Selected Dates ( Excludes 3PL))'
    file_link = 'not provided'


# endregion

# endregion

# region Smartsheet

class TransfersFileSS(DataFile):
    def __init__(self, df):
        super().__init__(df)

    file_description = 'All Smartsheet transfer lines (All divisions, All time)'
    file_link = 'not provided'


# endregion

# region Accuv

class MaterialStatusFile(DataFile):
    def __init__(self, df):
        super().__init__(df)

    file_description = 'All Material for selected Job numbers'
    file_link = 'not provided'


# endregion

# region EShipping

class ShipmentsFile(DataFile):
    def __init__(self, df):
        super().__init__(df)

    file_description = 'Shipments documented on eShipping website'
    file_link = 'not provided'


# endregion

# region Utility

class FileDetection:
    def __init__(self):
        self.missing_files = list()

    def add_dfo(self, dfo):
        self.missing_files.append(dfo)

    def read_dfo_list(self):
        if len(self.missing_files) > 0:
            for x in self.missing_files:
                print(x.file_description)
                print(x.file_link)
                print(get_data_file_types()[x])

# endregion

# endregion

# region Functions


def get_df(path):
    if path.endswith('.xlsx') \
            or path.endswith('.xls') \
            or path.endswith('.xlsm') \
            or path.endswith('.xlsb') \
            or path.endswith('.odf') \
            or path.endswith('.ods ') \
            or path.endswith('.odt'):
        return pd.read_excel(path)

    elif path.endswith('.csv'):
        return pd.read_csv(path, encoding='unicode_escape')
    else:
        return False


def create_data_file_object_1(path):
    # Verifies that the selected file is data file
    if os.path.isfile(path):
        if path.endswith('csv'):
            for key, value in get_data_file_types().items():
                if set(get_column_values(path)) == set(value):
                    break
            else:
                return False
    else:
        return False

    # Determines what type of data file object the selected file is
    df = get_df(path)
    if df is not False:
        data_file_object = DataFile(df)
        while len(data_file_object.__class__.__subclasses__()) > 0:
            data_file_object = create_data_file_object_2(df, data_file_object)
        return data_file_object
    else:
        return False


def create_data_file_object_2(df, data_file_object):
    if not isinstance(df, pd.DataFrame):
        return False
    else:
        id_values = data_file_object.id_type
        data_file_types = get_data_file_types()
        if isinstance(id_values, numpy.ndarray):
            for key, value in data_file_types.items():
                if set(value) == set(id_values):
                    return key(df)

        elif isinstance(id_values, dict):
            for key, value in data_file_types.items():
                if isinstance(value, dict):
                    if value == id_values:
                        return key(df)
        else:
            return False


def div_market(m_d):
    r_list = list()
    for x in m_d:
        if 'ALS30' in x:
            r_list.append('MOKA')
        elif 'MOKA' in x:
            r_list.append('ALS30')

    return r_list


def get_data_file(data_file_object, pref_type='most recent', files=True, pickled=True, markets=None):
    global file_detection_global
    global data_files_global
    data_files = [x for x in data_files_global if isinstance(x, data_file_object)]
    if len(data_files) > 0:
        if pref_type == 'most recent':
            dfo = max(data_files, key=attrgetter('dl_date'))
            dfo.filter_markets(markets)
            return dfo
        else:
            pass
    else:
        file_detection_global.add_dfo(data_file_object)
        return None


def find_data_files(data_file_object=DataFile, directory=os.getcwd(), pickled=True, files=True, current_date=True):
    if pickled is True:
        try:
            data_files = [x for x in pickle.load(open('datafiles.pl', 'rb')) if isinstance(x, data_file_object)]
        except FileNotFoundError:
            data_files = list()
    else:
        data_files = list()

    for x in os.listdir(directory):
        if x == 'data_files':
            data_files.extend(find_data_files(data_file_object, os.getcwd() + '/' + x))
        else:
            data_file = create_data_file_object_1(directory + '/' + x)
            if isinstance(data_file, data_file_object):

                if current_date is True:
                    if data_file.validate() is True:
                        data_files.append(data_file)
                else:
                    data_files.append(data_file)

    return data_files


def get_data_file_types():
    return pickle.load(open('datafiletypes.pl', 'rb'))


def validate_pickled_data_files(remove=True):
    data_files = [x for x in find_data_files(DataFile, files=False) if x in get_data_file_types()]
    pickle.dump(data_files, open('datafiles.pl', 'wb'))


# Manually define the values of a data file
def define_data_file_object(path, data_file_type):
    try:
        dft = get_data_file_types()
        try:
            dft.pop(data_file_type)
        except KeyError:
            pass
        if len(data_file_type.__subclasses__()) > 0:
            input_data_file_type = data_file_type.__base__
        else:
            input_data_file_type = data_file_type

        dft.update({data_file_type: input_data_file_type(get_df(path)).id_type})
        pickle.dump(dft, open('datafiletypes.pl', 'wb'))
    except FileNotFoundError:
        pickle.dump({data_file_type: get_df(path).columns.values}, open('datafiletypes.pl', 'wb'))


def get_column_values(path):
    try:
        a = open(path, 'r').readline()
        b = re.findall(r'"(.*?)"', a)

        if len(b) > 0:
            for x in b:
                c = re.sub(r'"' + re.escape(x) + r'"', "", a)
                c = re.sub(r',,', ',', c)
        else:
            c = copy.copy(a)

        c = c.split(',')
        c = [x.strip("'\n'") for x in c]
        c = [x.strip('"') for x in c]

        if len(b) > 0:
            c.extend(b)

        return c
    except UnicodeDecodeError:
        return pd.read_excel(path).columns.values


# endregion


# region Globals
file_detection_global = FileDetection()
data_files_global = find_data_files()
# endregion
