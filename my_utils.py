from cloudreve import CloudreveV4
import urllib3
import argparse
from tqdm import tqdm
import os
from pprint import pprint
from pathlib import Path


class Utils:
    def __init__(self,conn):
        self.conn = conn

    def tree(self,remote_path):
        dir_info_dict = self.conn.list(remote_path)
        files_info_dict = dir_info_dict['files']
        path_list = []
        for dict_i in files_info_dict:
            path_origin = dict_i['path']
            path = path_origin.replace('cloudreve://my', '')
            f_type = dict_i['type']
            if f_type == 0:  # file
                path_list.append(path)
            elif f_type == 1:  # folder
                path_list_i = self.tree(path)
                path_list += path_list_i
            else:
                raise Exception('unknown file type')
        return path_list

    def check_is_file(self,remote_path):
        info = self.conn.get_info(remote_path)
        f_type = info['type']
        if f_type == 0: return True
        elif f_type == 1: return False
        else: raise Exception('unknown file type')
