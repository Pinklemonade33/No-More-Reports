import string
from report_objects import *
from data_file_objects import *
import random
from datetime import datetime, timedelta


def fill_random_no_copies(_data, amount):
    values = list()
    for x in range(0, amount):
        val = None
        while val is None or val in values:
            val = random.choice(_data)
        values.append(val)
    return values


def generate_acronyms(letter_amount, amount):
    acronyms = list()
    for x in range(0, amount):
        letters = str()
        for y in range(0, letter_amount):
            letters += (random.choice(string.ascii_uppercase))
        acronyms.append(letters)

    return acronyms


def generate_number_combos(number_amount, amount):
    numbers_combos = list()
    for x in range(0, amount):
        numbers = str()
        for y in range(0, number_amount):
            numbers += str((round(random.uniform(0, 9))))
        numbers_combos.append(numbers)

    return numbers_combos


def generate_item_numbers():
    seg_1 = list()
    for x in range(2, 7):
        seg_1.extend(generate_acronyms(x, 25))

    seg_2 = list()
    for x in range(3, 7):
        seg_2.extend(generate_number_combos(x, 25))

    seg_3 = list()
    for x in range(1, 3):
        seg_3.extend(generate_acronyms(x, 25))

    items = list()
    for x in range(0, 100):
        p1 = random.choice(seg_1)
        p2 = random.choice(seg_2)
        p3 = random.choice(seg_3)
        items.append(
            p1 + '-' + p2 + '-' + p3
        )

    return items


def generate_values(amount, name_base):
    values = list()
    for x in range(1, amount + 1):
        values.append(name_base + '_' + str(x))

    return values


def create_dates():
    start_date = datetime(2022, 1, 1)
    end_date = datetime(2022, 12, 31)
    _dates = []

    current_date = start_date
    while current_date <= end_date:
        _dates.append(current_date)
        current_date += timedelta(days=1)

    return _dates


def duplicate_to_fill(values, amount):
    new_values = list()
    for x in range(1, amount + 1):
        val = random.choice(values)
        new_values.append(val)

    return new_values


def generate_quantities(amount):
    quantities = []
    for x in range(1, amount + 1):
        quantities.append(round(random.uniform(1, 5000)))

    return quantities


def generate_sequence_numbers(amount, originating=False, originating_seq=None):
    sequences = list()
    if originating is False:
        half = int(amount/2)
        for x in range(1, half + 1):
            number = None
            while number in sequences or number is None:
                number = round(random.uniform(10000, 99999))

            sequences.append(number)

        seq = [x for x in originating_seq if x is not None]
        seq.extend(sequences)
        return seq

    else:
        half = int(amount/2)
        for x in range(1, half + 1):
            sequences.append(None)
        for x in range(1, half + 1):
            number = None
            while number in sequences or number is None:
                number = round(random.uniform(10000, 99999))

            sequences.append(number)

        return sequences


def generate_INVLocationsFile(inv=None):
    if inv is None:
        locations = generate_locations()
        line_amount = len(locations)
        items = fill_random_no_copies(generate_item_numbers(), line_amount)
        division = generate_acronyms(5, 20)[0]
        divisions = [division for x in range(0, line_amount)]
    else:
        locations = inv['locations']
        line_amount = len(locations)
        items = fill_random_no_copies(inv['items'], line_amount)
        division = inv['divisions'][0]
        divisions = [division for x in range(0, line_amount)]

    quantities = generate_quantities(line_amount)
    dates = duplicate_to_fill(create_dates(), line_amount)
    df = pd.DataFrame(columns=get_data_file_types()[INVLocationsFile])
    df['Location ID'] = locations
    df['Item Number Text'] = items
    df['Quantity'] = quantities
    df['Inventory Item by Location: Last Modified Date'] = dates
    df['Last Issue Date'] = dates
    df['Serial Number'] = ''
    df['Division'] = divisions

    return df


def generate_TransactionsFile(transaction_type='div', inv=None, line_amount=50):
    if inv is None:
        items = fill_random_no_copies(generate_item_numbers(), line_amount)
        locations = duplicate_to_fill(generate_locations(), line_amount)

        if transaction_type == 'loc':
            transaction_ids = ['INVLOCLOC' for x in range(1, line_amount + 1)]
            divisions = duplicate_to_fill(generate_acronyms(4, 20), line_amount)[0]
        else:
            transaction_ids = ['INVDIVDIV' for x in range(1, line_amount + 1)]
            divisions = duplicate_to_fill(generate_acronyms(4, 20), line_amount)
    else:
        items = fill_random_no_copies(inv['items'], line_amount)
        locations = duplicate_to_fill(inv['locations'], line_amount)
        if transaction_type == 'loc':
            transaction_ids = ['INVLOCLOC' for x in range(1, line_amount + 1)]
            divisions = inv['divisions'][0]
        else:
            transaction_ids = ['INVDIVDIV' for x in range(1, line_amount + 1)]
            divisions = duplicate_to_fill(inv['divisions'], line_amount)

    originating_seq_num = generate_sequence_numbers(line_amount, True)
    seq_num = generate_sequence_numbers(line_amount, False, originating_seq_num)
    quantities = generate_quantities(line_amount)
    users = generate_values(5, 'User')
    dates = duplicate_to_fill(create_dates(), line_amount)

    df = pd.DataFrame(columns=get_data_file_types()[TransactionsFile])
    df['Sequence No'] = seq_num
    df['Originating Sequence No'] = originating_seq_num
    df['Date/Time'] = dates
    df['Transaction ID'] = transaction_ids

    for index, row in df.iterrows():

        item = random.choice(items)
        qty = random.choice(quantities)
        if transaction_type == 'loc':
            loc_1 = random.choice(locations)
            loc_2 = None
            while loc_2 == loc_1 or loc_2 is None:
                loc_2 = random.choice(locations)

            div_1 = divisions
            div_2 = divisions

        else:
            div_1 = random.choice(divisions)
            div_2 = None
            while div_1 == div_2 or div_2 is None:
                div_2 = random.choice(divisions)

            loc_1 = 'DTIB'
            loc_2 = 'DTOB'

        user = random.choice(users)
        df.at[index, 'Item Number'] = item
        df.at[index, 'Stock Loc ID'] = loc_1
        df.at[index, 'Transaction Qty'] = qty
        df.at[index, 'User Number'] = user
        df.at[index, 'locdiv'] = div_1

        for index_2, row_2 in df.iterrows():

            if index != index_2 \
                and row['Sequence No'] == row_2['Originating Sequence No'] \
                    or row['Originating Sequence No'] == row_2['Sequence No']:

                df.at[index_2, 'Stock Loc ID'] = loc_2
                df.at[index_2, 'Item Number'] = item
                df.at[index_2, 'Transaction Qty'] = qty
                df.at[index_2, 'User Number'] = user
                df.at[index_2, 'locdiv'] = div_2

    return df


def generate_job_numbers(amount):
    seg_1 = fill_random_no_copies(generate_acronyms(5, 20), 3)
    job_numbers = list()
    for x in seg_1:
        seg_2 = generate_number_combos(5, 20)
        for y in seg_2:
            job_numbers.append(x + '_' + y)
    return job_numbers


def generate_site_names(amount):
    seg_1 = fill_random_no_copies(generate_acronyms(4, 20), 3)
    site_names = list()
    for x in seg_1:
        seg_2 = generate_number_combos(4, 20)
        for y in seg_2:
            site_names.append(x + y)
    return site_names


def generate_MaterialStatusFile(inv=None, line_amount=50):
    job_numbers = generate_job_numbers(line_amount)
    site_names = generate_site_names(line_amount)
    if inv is None:
        items = generate_values(30, 'Item')
    else:
        items = inv['items']
    jobs = list()
    sites = list()
    items_total = list()
    quantities = list()

    for job, site in zip(job_numbers, site_names):
        items_2 = list()
        for x in range(1, round(random.uniform(5, 10))):
            r_item = None

            while r_item in items_2 or r_item is None:
                r_item = random.choice(items)

            items_2.append(r_item)

        jobs.extend([job for x in items_2])
        sites.extend([site for x in items_2])
        quantities.extend([round(random.uniform(1, 1000)) for x in items_2])
        items_total.extend(items_2)

    data = {key: list() for key in get_data_file_types()[MaterialStatusFile]}
    data['Job #'] = jobs
    data['Job Name'] = sites
    data['Item #'] = items_total
    data['Firm Qty'] = quantities

    for key, value in data.items():
        if len(data[key]) == 0:
            data[key] = [np.nan for x in jobs]
    df = pd.DataFrame(data)

    return df


def generate_locations(loc_range_x=6, loc_range_y=6, loc_range_z=3, overstock_at=3,
                       start_x=0, start_y=0, start_z=0, length=10, width=10, height=10,
                       s_type='static', buffer_x=0, buffer_y=0,
                       buffer_z=0, loc_type='lln'):

    current_x = start_x
    locations = list()
    ending_segment = 'Picking Lane'

    for x in range(1, loc_range_x):
        if x > 1:
            current_x += length + 1 + buffer_x
        if x == overstock_at:
            ending_segment = 'Overstock'

        current_y = start_y

        if loc_type[0] == 'l':
            x_loc = chr(ord('@') + x)
        elif loc_type[0] == 'n':
            x_loc = x + 1
        else:
            raise ValueError

        for y in range(1, loc_range_y):
            if y > 1:
                current_y += width + 1 + buffer_y
            current_z = start_z

            if loc_type[1] == 'l':
                y_loc = chr(ord('@') + y)
            elif loc_type[1] == 'n':
                y_loc = y + 1
            else:
                raise ValueError

            for z in range(loc_range_z):
                if z > 1:
                    current_z += height + 1 + buffer_z

                if loc_type[2] == 'l':
                    z_loc = chr(ord('@') + z)
                elif loc_type[2] == 'n':
                    z_loc = z + 1
                else:
                    raise ValueError

                name = str(x_loc) + str(y_loc) + str(z_loc) + ' ' + ending_segment
                locations.append(name)

    return locations


def generate_po(amount=200):
    seg_1 = list()
    for x in range(1, 2):
        n_1 = round(random.uniform(1, 9))
        n_2 = round(random.uniform(1, 9))
        seg_1.append(str(n_1) + str(n_2))

    seg_3 = list()
    for x in range(1, 15):
        n_1 = round(random.uniform(1, 9))
        n_2 = round(random.uniform(1, 9))
        n_3 = round(random.uniform(1, 9))
        seg_3.append(str(n_1) + str(n_2) + str(n_3))

    seg_4 = list()
    for x in range(1, 15):
        n_1 = round(random.uniform(1, 9))
        n_2 = round(random.uniform(1, 9))
        seg_4.append(str(n_1) + str(n_2))
    po_numbers = list()
    for x in range(0, amount):
        po = str('PO-' + random.choice(seg_1)) + '00000' + str(random.choice(seg_3)) + '-' + str(random.choice(seg_4))
        po_numbers.append(po)

    return po_numbers


def share_fill_random(share_min, share_max, obj_amount, data, amount):
    amount_shared = round(random.uniform(share_min, share_max))
    shared_data = list()
    all_data = list()
    for x in range(1, amount_shared + 1):
        shared_data.append(random.choice(data))

    all_data.extend(shared_data)
    objects = list()
    amount_left = amount - amount_shared
    for x in range(1, obj_amount + 1):
        obj = all_data.copy()
        for y in range(1, amount_left + 1):
            obj.append(random.choice(data))

        objects.append(obj)

    return objects


def generate_PoLineFile(pending, line_amount, inv=None):
    if inv is None:
        items = duplicate_to_fill(fill_random_no_copies(generate_item_numbers(), 100), line_amount)
        po_numbers = fill_random_no_copies(generate_po(), line_amount)
        dates = duplicate_to_fill(create_dates(), line_amount)
        divisions = generate_acronyms(5, 20)
    else:
        items = inv['items']
        line_amount = len(items)
        po_numbers = fill_random_no_copies(inv['po'], line_amount)
        dates = duplicate_to_fill(inv['dates'], line_amount)
        divisions = inv['divisions']

    if pending is False:
        quantities = [generate_quantities(line_amount), [0 for x in range(0, line_amount)]]
    else:
        quantities = share_fill_random(0, 0, 2, generate_quantities(line_amount), line_amount)

    qty_received = quantities[0]
    qty_remaining = quantities[1]

    division = ''.join(str(x) for x in divisions[0])

    data = {key: list() for key in get_data_file_types()[PoLineFile]}
    data['PO Line: Name'] = po_numbers
    data['Dock Date'] = dates
    data['Qty Received'] = qty_received
    data['Qty Remaining, PO Line'] = qty_remaining
    data['Item: Name'] = items
    data['Division'] = [division for x in range(0, line_amount)]

    for key, value in data.items():
        if len(data[key]) == 0:
            data[key] = [np.nan for x in items]

    return pd.DataFrame(data)


def generate_ShipmentsFile(inv=None, line_amount=50):
    if inv is None:
        origin_names = generate_acronyms(5, 20)
        ref_numbers = fill_random_no_copies(generate_po(), line_amount)
        customer_names = generate_values(50, 'customer_name')
        dates = duplicate_to_fill(create_dates(), 50)
        bill_number = generate_number_combos(10, 50)
    else:
        ref_numbers = inv['po'].copy()
        line_amount = len(ref_numbers)
        origin_names = [random.choice(inv['divisions']) for x in range(0, line_amount)]
        customer_names = [random.choice(inv['divisions']) for x in range(0, line_amount)]
        dates = duplicate_to_fill(inv['dates'], line_amount)
        bill_number = generate_number_combos(10, line_amount)

    data = {key: list() for key in get_data_file_types()[ShipmentsFile]}
    data['Reference Numbers'] = ref_numbers
    data['BillNumber'] = bill_number
    data['Origin Name'] = origin_names
    data['Customer Name'] = customer_names
    data['Estimated Delivery Date'] = dates

    for key, value in data.items():
        if len(data[key]) == 0:
            data[key] = [np.nan for x in customer_names]

    return pd.DataFrame(data)


def compare_cols(df, data_file_type):
    a = [x for x in get_data_file_types()[data_file_type] if x not in df.columns.values]
    b = [x for x in df.columns.values if x not in get_data_file_types()[data_file_type]]
    c = [x for x in df.columns.values]
    d = [x for x in get_data_file_types()[data_file_type]]

    return [a, b, c, d]


def generate_all_files(inv, write_new=False):
    dfs = [
        generate_INVLocationsFile(inv),
        generate_PoLineFile(False, 50, inv),
        generate_PoLineFile(True, 50, inv),
        generate_ShipmentsFile(inv, 50),
        generate_MaterialStatusFile(inv),
        generate_TransactionsFile('div', inv),
        generate_TransactionsFile('loc', inv),
    ]

    dfs[0].to_csv('InvLocationsFile.csv', index=False)
    dfs[1].to_csv('ReceivedPoLineFile.csv', index=False)
    dfs[2].to_csv('PendingPoLineFile.csv', index=False)
    dfs[3].to_csv('ShipmentsFile.csv', index=False)
    dfs[4].to_csv('MaterialStatusFile.csv', index=False)
    dfs[5].to_csv('DivTransactionsFile.csv', index=False)
    dfs[6].to_csv('LocTransactionsFile.csv', index=False)

    return dfs


inv_data = {
    'items': generate_item_numbers(),
    'locations': generate_locations(),
    'po': generate_po(),
    'divisions': generate_acronyms(5, 20),
    'dates': create_dates(),
}

df_ = POShipping()
