from data_file_objects import *


def create_list_file(path, list_file_type, alt=None):
    lst = get_df(path)['Item Number Text'].to_list()
    list_files = get_list_file()
    list_file = get_list_file(list_file_type)
    if alt is None:
        for x in lst:
            list_file.append(x)
    elif alt == 'change':
        list_file = lst

    list_files.update({list_file_type: list(set(list_file))})
    pickle.dump(list_files, open('listfiles.pl', 'wb'))


def get_list_file(list_file_type=None):
    try:
        list_files = pickle.load(open('listfiles.pl', 'rb'))
    except FileNotFoundError:
        list_files = dict()

    if list_file_type is None:
        return list_files
    else:
        for key, value in list_files.items():
            if key == list_file_type:
                return value
        else:
            return list()


def list_file_add(list_file_type, item):
    lst = get_list_file(list_file_type)
    lst.append(item)
    list_files = get_list_file()
    list_files.update({list_file_type: list(set(lst))})
    pickle.dump(list_files, open('listfiles.pl', 'wb'))


def list_file_remove(list_file_type, item):
    lst = get_list_file(list_file_type)
    lst.remove(item)
    list_files = get_list_file()
    list_files.update({list_file_type: list(set(lst))})
    pickle.dump(list_files, open('listfiles.pl', 'wb'))


def get_non_pick_items():
    categories = ['steel', 'ant.', 'wire']
    a = list()
    for c in categories:
        a.extend(get_list_file(c))

    return a


def list_file_remove_type(list_file_type):
    lst = get_list_file()
    lst.pop(list_file_type)
    pickle.dump(lst, open('listfiles.pl', 'wb'))

