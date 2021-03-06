from os import path
import csv

import yaml


def in_module_path(filename):
    return path.join(path.dirname(path.realpath(__file__)), filename)


def load_yaml(filename):
    with open(filename, 'r') as f:
        return yaml.load(f.read())


maps = load_yaml(in_module_path('maps.yaml'))


def load_tariffs_map(filename):
    with open(filename) as csvfile:
        data = {}
        for row in csv.DictReader(csvfile, delimiter=';'):
            try:
                data[int(row['Старый ТП'])] = int(row['Новый ТП'])
            except ValueError:
                pass
        return data


def load_tariffs_policy_map(filename):
    with open(filename) as csvfile:
        data = {}
        for row in csv.DictReader(csvfile, delimiter=';'):
            name = row['Наименование'].replace('HIGH_', '')
            policy_id = int(row['RESCONNID'])
            data[name] = policy_id
        return data


def load_groups_map(filename):
    with open(filename) as csvfile:
        data = {}
        for row in csv.DictReader(csvfile, delimiter=';'):
            data[row['Название группы']] = int(row['Группа'])
        return data


def load_sitename_to_usrconnid_map(filename):
    fieldnames = maps['export-files']['connections-list'][1].split(';')
    with open(filename) as csvfile:
        data = {}
        for row in csv.DictReader(csvfile, fieldnames=fieldnames, delimiter=';'):
            data[row['SITENAME']] = int(row['USRCONNID'])
        return data


def load_profile(filename):
    profile_dir = path.dirname(path.realpath(filename))
    profile = load_yaml(filename)

    parent = {}
    if 'include' in profile:
        parent_filename = path.join(profile_dir, profile['include'])
        parent = load_profile(parent_filename)

    if 'tariffs-map-file' in profile:
        map_filename = path.join(profile_dir, profile['tariffs-map-file'])
        profile['tariffs-map'] = load_tariffs_map(map_filename)

    if 'tariffs-policy-map-file' in profile:
        map_filename = path.join(profile_dir, profile['tariffs-policy-map-file'])
        profile['tariffs-policy-map'] = load_tariffs_policy_map(map_filename)

    if 'groups-map-file' in profile:
        map_filename = path.join(profile_dir, profile['groups-map-file'])
        profile['groups-map'] = load_groups_map(map_filename)

    parent.update(profile)
    return parent
