import pickle
from data_file_objects import *
from list_file_methods import *
import pandas as pd
from datetime import datetime, date, timedelta
import numpy as np

# region Single Column Reports


class LocationID:
    def __init__(self, parent, loc_keyword):
        self.df = parent.df
        self.loc_keyword = loc_keyword

    def _list_handler(self, method):
        print(method)
        dfs = list()
        for x in self.loc_keyword:
            dfs.append(method(x))
        return pd.concat(dfs)

    def get_overstock(self, keyword=None):
        if keyword is None:
            keyword = self.loc_keyword

        if isinstance(keyword, str):
            return self.df.loc[self.df[keyword].str.contains('Overstock') == True]
        elif isinstance(keyword, list):
            return self._list_handler(self.get_overstock)

    def get_picking(self, keyword=None):
        if keyword is None:
            keyword = self.loc_keyword

        if isinstance(keyword, str):
            return self.df.loc[self.df[keyword].str.contains('Picking') == True]
        elif isinstance(keyword, list):
            return self._list_handler(self.get_picking)

    def get_steel_loc(self, keyword=None):
        if keyword is None:
            keyword = self.loc_keyword

        if isinstance(keyword, str):
            return self.df.loc[(self.df[keyword].str.contains('Yrd2') == True) |
                               (self.df[keyword].str.contains('YARD') == True) |
                               (self.df[keyword].str.contains('Wall') == True)]
        elif isinstance(keyword, list):
            return self._list_handler(self.get_steel_loc)

    def get_wire_loc(self, keyword=None):
        if keyword is None:
            keyword = self.loc_keyword

        if isinstance(keyword, str):
            return self.df.loc[self.df[keyword].str.contains('WIRE') == True]
        elif isinstance(keyword, list):
            return self._list_handler(self.get_wire_loc)

    def get_ant_loc(self, keyword=None):
        if keyword is None:
            keyword = self.loc_keyword

        if isinstance(keyword, str):
            return self.df.loc[self.df[keyword].str.contains('ANT.') == True]
        elif isinstance(keyword, list):
            return self._list_handler(self.get_ant_loc)
# endregion

# region Single File Reports


class TransfersSS:
    def __init__(self, top=True):
        self.dfo_type = TransfersFileSS
        self.dfo = get_data_file(self.dfo_type)

        if self.dfo is not None:
            self.df = self.dfo.df
        else:
            self.df = None

        if top:
            file_detection_global.read_dfo_list()

    def get_bad_blanks(self):
        r_ids = list()
        df = self.df

        filled_df = df.loc[df['Pick Completed'] == 1.0]
        bad_df = filled_df[filled_df['Physical Transfer QTY'].isnull()]
        r_ids.extend(bad_df['Request ID'].values.tolist())

        filled_df = df.loc[df['Shipment Requested'] == 1.0]
        bad_df = filled_df[(filled_df['Physical Transfer QTY'].isnull()) |
                           (filled_df['Pick Completed'].isnull()) |
                           (filled_df['Tracking Information'].isnull())]
        r_ids.extend(bad_df['Request ID'].values.tolist())

        filled_df = df.loc[df['Shipment Picked Up'] == 1.0]
        bad_df = filled_df[(filled_df['Physical Transfer QTY'].isnull()) |
                           (filled_df['Pick Completed'].isnull()) |
                           (filled_df['Tracking Information'].isnull()) |
                           (filled_df['Shipment Requested'].isnull())]
        r_ids.extend(bad_df['Request ID'].values.tolist())

        filled_df = df.loc[(df['Physical Transfer QTY'].notnull()) |
                           (df['Shipment Picked Up'].notnull()) |
                           (df['Tracking Information'].notnull()) |
                           (df['Shipment Requested'].notnull())]
        bad_df = filled_df[filled_df['Pick Completed'].isnull()]
        r_ids.extend(bad_df['Request ID'].values.tolist())

        filled_df = df.loc[(df['Physical Transfer QTY'].notnull()) |
                           (df['Shipment Picked Up'].notnull()) |
                           (df['Tracking Information'].notnull()) |
                           (df['Pick Completed'].notnull())]
        bad_df = filled_df[filled_df['Shipment Requested'].isnull()]
        r_ids.extend(bad_df['Request ID'].values.tolist())

        filled_df = df.loc[(df['Physical Transfer QTY'].notnull()) |
                           (df['Shipment Requested'].notnull()) |
                           (df['Tracking Information'].notnull()) |
                           (df['Pick Completed'].notnull())]
        bad_df = filled_df[filled_df['Shipment Picked Up'].isnull()]
        r_ids.extend(bad_df['Request ID'].values.tolist())

        return r_ids

    def get_canceled(self):
        return self.df.loc[self.df['Transfer QTY'] == 0]

    def get_canceled_after_done(self):
        return self.df.loc[(self.df['Transfer QTY'] == 0) &
                           ((self.df['Physical Transfer QTY'] != 0) |
                            (self.df['Pick Completed'].notnull()))]

    def get_tracking_dtib_incomplete(self):
        ss_df_t = self.df.loc[(self.df['Tracking Information'].notnull()) &
                              (self.df['Completed'].isnull())]

        tracking = ss_df_t['Tracking Information'].tolist()
        tracking = list(set(tracking))
        e_ship = [x for x in tracking if x[0] < 7]
        fedex = [x for x in tracking if x[0] >= 7]

        return_dict = {'e_ship': e_ship, 'fedex': fedex}
        return return_dict


class DIVTransactionsRS:
    def __init__(self, top=True):
        self.dfo_type = DIVTransactionsFile
        self.dfo = get_data_file(self.dfo_type)

        if self.dfo is not None:
            self.df = self._initialize_df()
            self.loc_id = LocationID(self, ['from location', 'to location'])
        else:
            self.df = None

        if top:
            file_detection_global.read_dfo_list()

    def _initialize_df(self):
        df = self.dfo.df
        df = df.loc[df['Transaction ID'].notnull()]
        df1 = df.loc[df['Originating Sequence No'].isnull()].copy()
        df2 = df.loc[df['Originating Sequence No'].notnull()].copy()
        df2['Originating Sequence No'] = df2['Originating Sequence No'].astype(int)
        df1['Sequence No'] = df1['Sequence No'].astype(int)
        df2 = df2[['Originating Sequence No', 'locdiv', 'Stock Loc ID']]
        df2 = df2.rename(
            columns={'Originating Sequence No': 'Sequence No', 'Stock Loc ID': 'to location', 'locdiv': 'to division'})
        df2 = df2.set_index('Sequence No')
        df1 = df1.set_index('Sequence No')
        df1 = df1.iloc[:, 1:]
        df1 = df1.rename(columns={'Stock Loc ID': 'from location', 'locdiv': 'from division'})
        df1 = df1.join(df2)
        cols = df1.columns.values.tolist()
        cols.remove('to division')
        cols.remove('to location')
        cols.insert(2, 'to division')
        cols.insert(4, 'to location')
        return df1.reindex(columns=cols)

    def get_from_div(self, div):
        return self.df.loc[(self.df['Stock Loc ID'] == div) & (self.df['locid'] == 'DTOB')]

    def get_to_div(self, div):
        return self.df.loc[(self.df['Stock Loc ID'] == div) & (self.df['locid'] == 'DTIB')]


class LOCTransactionsRS:
    def __init__(self, top=True):
        self.dfo_type = LOCTransactionFile
        self.dfo = get_data_file(self.dfo_type)

        if self.dfo is not None:
            self.df = self._initialize_df()
            self.loc_id = LocationID(self, ['from location', 'to location'])
        else:
            self.df = None

        if top:
            file_detection_global.read_dfo_list()

    def _initialize_df(self):
        df = self.dfo.df.copy()
        df = df.loc[df['Transaction ID'].notnull()]
        df1 = df.loc[df['Originating Sequence No'].isnull()].copy()
        df2 = df.loc[df['Originating Sequence No'].notnull()].copy()
        df2['Originating Sequence No'] = df2['Originating Sequence No'].astype(int)
        df1['Sequence No'] = df1['Sequence No'].astype(int)
        df2 = df2[['Originating Sequence No', 'locdiv', 'Stock Loc ID']]
        df2 = df2.rename(
            columns={'Originating Sequence No': 'Sequence No', 'Stock Loc ID': 'to location', 'locdiv': 'to division'})
        df2 = df2.set_index('Sequence No')
        df1 = df1.set_index('Sequence No')
        df1 = df1.iloc[:, 1:]
        df1 = df1.rename(columns={'Stock Loc ID': 'from location', 'locdiv': 'from division'})
        df1 = df1.join(df2)
        cols = ['Item Number', 'from division', 'to division', 'from location', 'to location', 'Transaction Qty',
                'Date/Time', 'Transaction ID', 'User Number']
        df1['Transaction Qty'] = df1['Transaction Qty'].astype(int)
        return df1.reindex(columns=cols)


class INVLocationsRS:
    def __init__(self, top=True):
        self.dfo_type = INVLocationsFile
        self.dfo = get_data_file(self.dfo_type)

        if self.dfo is not None:
            self.df = self.dfo.df
            self.loc_id = LocationID(self, 'Location ID')
        else:
            self.df = None

        if top:
            file_detection_global.read_dfo_list()

    def get_aging_dtob(self, days):
        if days != 0:
            last_date = datetime.now() - timedelta(days=days)
        else:
            last_date = datetime.now() + timedelta(days=1)
        dtob = self.df.loc[(self.df['Location ID'].str.contains('DTOB')) &
                           (self.df['Inventory Item by Location: Last Modified Date'].astype('datetime64[ns]')
                            < last_date)]

        return dtob


class MaterialStatus:
    def __init__(self, top=True):
        self.dfo_type = MaterialStatusFile
        self.dfo = get_data_file(self.dfo_type)

        if self.dfo is not None:
            self.df = self.dfo.df
        else:
            self.df = None

        if top:
            file_detection_global.read_dfo_list()

    def get_material_quantities(self):
        return self.df.groupby(['Item #']).sum(numeric_only=True)['Firm Qty']


class Shipments:
    def __init__(self, top=True):
        self.dfo_type = ShipmentsFile
        self.dfo = get_data_file(self.dfo_type)

        if self.dfo is not None:
            self.df = self._initialize_df()
        else:
            self.df = None

        if top:
            file_detection_global.read_dfo_list()

    def _initialize_df(self):
        def get_last_main_po_index(po):
            for index, value in enumerate(po):
                if index > 2:
                    if value == '/' or value == '-':
                        return index

        def get_main_po(po, full=True):
            if full is True:
                return po[:get_last_main_po_index(po)]
            elif full is False:
                return po[3:get_last_main_po_index(po)]

        def get_po_lines(po):
            po2 = po[get_last_main_po_index(po):]
            po2 = po2.replace('/', '-')
            po2 = po2.split('-')
            return ['-' + s for s in po2 if s != '' and s != get_main_po(po, full=False)]

        def reformat_pos(po):
            return [get_main_po(po) + s for s in get_po_lines(po)]

        def reformat_po_list(po_list):
            new_list = list()
            for x in po_list:
                new_list.extend(reformat_pos(x))
            return new_list

        def reformat_po_df(df):
            new_dict = dict()
            for x in df.itertuples():
                new_list = [y for y in x if isinstance(y, str)]
                new_list = [y for y in new_list if 'PO' in y]
                new_list = [y[y.find('PO'):] for y in new_list]
                new_list = [y for y in new_list if y[y.find('PO') + 2] in ['-', ' ', '/']]
                new_list = reformat_po_list(new_list)
                new_dict.update({x.Index: new_list})
            return new_dict

        df1 = self.dfo.df.copy()
        df2 = df1.loc[df1['Reference Numbers'].str.contains('PO')]
        df2 = df2['Reference Numbers'].str.split(';', expand=True)
        df1 = df1[['BillNumber', 'Origin Name', 'Customer Name', 'Estimated Delivery Date']]
        df1['PO Numbers'] = reformat_po_df(df2)
        return df1.loc[df1['PO Numbers'].notnull()]

    def shipments_to_dict(self):
        new_dict = dict()
        for x in self.df.itertuples():
            for y in x[5]:
                new_dict.update({y: [x[1], x[2], x[3], x[4]]})
        return new_dict


class PendingPOLine:
    def __init__(self, markets=None, top=True):
        self.dfo_type = PendingPoLineFile
        self.dfo = get_data_file(self.dfo_type, markets=markets)

        if self.dfo is not None:
            self.df = self.dfo.df
        else:
            self.df = None

        if top:
            file_detection_global.read_dfo_list()


class ReceivedPOLine:
    def __init__(self, markets=None, top=True):
        self.dfo_type = ReceivedPoLineFile
        self.dfo = get_data_file(self.dfo_type, markets=markets)

        if self.dfo is not None:
            self.df = self.dfo.df
        else:
            self.df = None

        if top:
            file_detection_global.read_dfo_list()

# endregion

# region Cross File Reports


class MaterialForcast:
    def __init__(self, markets=None):
        self.mat_r = MaterialStatus(top=False)
        self.inv_r = INVLocationsRS(top=False)
        self.pending_po = PendingPOLine(top=False, markets=markets)
        if self.mat_r is not None and self.inv_r is not None and self.pending_po is not None:
            self.df_main = self._initialize_df_main()
            self.df_replen = self._initialize_df_replen()
            self.df_pending = self._initialize_df_pending()

        file_detection_global.read_dfo_list()

    def _initialize_df_main(self):
        mat_df = self.mat_r.df.copy()
        inv_df = self.inv_r.df.copy()
        req_items = mat_df['Item #'].to_list()
        non_pick = get_non_pick_items()
        items = [x for x in req_items if x not in non_pick]

        df1 = inv_df.loc[(inv_df['Location ID'].str.contains('Picking') == True) &
                         (inv_df['Item Number Text'].isin(items) == True)].groupby(
            ['Item Number Text']).sum(numeric_only=True)['Quantity']

        df2 = mat_df.loc[mat_df['Item #'].isin(items) == True].groupby(['Item #']).sum(numeric_only=True)['Firm Qty']
        df2 = df2.loc[(df2.notnull()) & (df2 != 0)]
        df2 = df2.to_frame().join(df1)
        df2['Needed From Overstock'] = df2['Quantity'] - df2['Firm Qty']
        df2.loc[df2['Quantity'].isnull(), 'Needed From Overstock'] = 0 - df2['Firm Qty']
        df2 = df2.loc[(df2['Needed From Overstock'] < 0) | (df2['Needed From Overstock'].isnull())]
        df2.loc[df2['Needed From Overstock'] < 0, 'Needed From Overstock'] *= -1
        rep_items = df2.index.values.tolist()
        s1 = self.inv_r.loc_id.get_overstock()
        s1 = s1.loc[s1['Item Number Text'].isin(rep_items) == True].groupby(['Item Number Text']).sum(numeric_only=True)[
            'Quantity']
        lst = s1.index.to_list()
        add_dict = {k: 0 for k in rep_items if k not in lst}
        add_s = pd.Series(data=add_dict)
        s1 = pd.concat([s1, add_s], ignore_index=False)
        s1 = s1.rename('Overstock Quantity')
        df3 = s1.to_frame()
        df3['Total Deficit'] = df3['Overstock Quantity'] - df2['Needed From Overstock']
        df3.loc[df3['Total Deficit'] >= 0, 'Total Deficit'] = 0
        df3.loc[df3['Total Deficit'] < 0, 'Total Deficit'] *= -1
        df2.loc[df2['Quantity'].isnull(), 'Quantity'] = 0
        df2 = df2.rename({'Quantity': 'In Picking Location'}, axis=1)
        return df2.join(df3)

    def _initialize_df_replen(self):
        lst = self.get_non_deficit().index.to_list()
        dct1 = self.get_non_deficit()['Needed From Overstock'].to_dict()
        df1 = self.inv_r.loc_id.get_overstock()
        df1 = df1.loc[df1['Item Number Text'].isin(lst) == True]
        df1 = df1.sort_values(by='Quantity')
        index_dict = dict()

        for index, column in df1.iterrows():
            if dct1[column['Item Number Text']] > 0:
                remaining = (column['Quantity'] - dct1[column['Item Number Text']]) * -1
                if column['Quantity'] - dct1[column['Item Number Text']] < 0:
                    take = (column['Quantity'] - dct1[column['Item Number Text']]) + dct1[column['Item Number Text']]
                else:
                    take = dct1[column['Item Number Text']]

                dct1[column['Item Number Text']] = remaining
                index_dict.update({index: take})

        df1 = df1.loc[index_dict.keys()][['Item Number Text', 'Location ID', 'Quantity']]
        df1 = df1.rename({'Quantity': 'Total Quantity'}, axis=1)
        s1 = pd.Series(index_dict, name='Take')
        df1 = df1.join(s1.to_frame())
        df1['Operator'] = np.where(df1['Take'] == df1['Total Quantity'], '=', '<=')
        df2 = pd.DataFrame(columns=['Actual Location', 'Actual Take'], index=list(index_dict.keys()), data='')
        df1 = df1.join(df2)
        return df1[
            ['Item Number Text', 'Location ID', 'Operator', 'Take', 'Actual Location', 'Actual Take', 'Total Quantity']]

    def get_deficit(self):
        return self.df_main.loc[self.df_main['Total Deficit'] > 0]['Total Deficit']

    def get_non_deficit(self):
        return self.df_main.loc[self.df_main['Total Deficit'] == 0]

    def _initialize_df_pending(self):
        s1 = self.get_deficit()
        pending_dict = s1.to_dict()
        df1 = self.pending_po.df.copy()
        # df1['Item: Name'] = df1['Item: Name'].str.split('(', n=1)
        # df1['Item: Name'] = df1['Item: Name'].str[0].str[:-1]
        df1 = df1.loc[df1['Item: Name'].isin(s1.index) == True]
        df1 = df1.sort_values(by=['Dock Date'], ascending=False)
        new_dict = {k: {'Quantity Needed': v,
                        'Quantity Accumulated': 0,
                        'Dock Date': None,
                        'PO': list()}
                    for k, v in pending_dict.items()}

        for x in df1.itertuples():
            if x[2] in new_dict.keys():
                y = new_dict[x[2]]
                if y['Quantity Accumulated'] < y['Quantity Needed']:
                    y['Quantity Accumulated'] += x[5]
                    y['Dock Date'] = x[6]
                    y['PO'].append(x[1])

        return pd.DataFrame(data=new_dict.values(), index=list(new_dict.keys()))


class POShipping:
    def __init__(self):
        self.po_pending = PendingPOLine(top=False)
        self.po_received = ReceivedPOLine(top=False)
        self.ship_report = Shipments(top=False)
        if self.po_pending is not None and self.po_received is not None and self.ship_report is not None:
            self.df = self._initialize_df()

        file_detection_global.read_dfo_list()

    def _initialize_df(self):
        ship_dict = self.ship_report.shipments_to_dict()
        df2 = pd.concat([self.po_received.df, self.po_pending.df], ignore_index=True)
        df2 = df2.loc[df2['PO Line: Name'].isin(ship_dict.keys()) == True]
        df2 = df2.set_index('PO Line: Name')
        ship_dict = {k: v for k, v in ship_dict.items() if k in df2.index.to_list()}
        df1 = pd.DataFrame(
            index=list(ship_dict.keys()), data=ship_dict.values(), columns=[[
                'BillNumber', 'Origin Name', 'Customer Name', 'Estimated Delivery Date']])

        return df2.join(df1)


# endregion
