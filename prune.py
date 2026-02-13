from cloudreve import CloudreveV4
from pathlib import Path
import os
import certifi
import argparse

os.environ["SSL_CERT_FILE"] = certifi.where()
os.environ["REQUESTS_CA_BUNDLE"] = certifi.where()


class Prune:

    def __init__(self,config_file=None):
        self.root_dir = '/_Transfer'
        if config_file is not None:
            self.config_file = Path.home() / ".config" / "cloudreve" / config_file
        else:
            self.config_file = Path.home() / ".config" / "cloudreve" / "passwd"
        if not os.path.isfile(self.config_file):
            # list files in Path.home() / ".config" / "cloudreve"
            config_file_list = sorted(os.listdir(Path.home() / ".config" / "cloudreve"))
            if len(config_file_list) == 0:
                print('not any config file found')
                print(f'please create a config file in the {Path.home() / ".config" / "cloudreve"}')
                exit()
            else:
                print(f'config file not found: {self.config_file}')
                print('please use -c to select a config file')

                print('config options:',' | '.join(config_file_list))
                exit()
            pass
        BASE_URL, username, password = self.get_passwd()
        self.conn = CloudreveV4(BASE_URL)
        self.conn.login(username, password)
        print('connected to', BASE_URL)
        pass

    def get_passwd(self):
        CONFIG_FILE = self.config_file
        login_info = open(CONFIG_FILE, 'r')

        login_info = login_info.read().splitlines()
        BASE_URL = login_info[0]
        username = login_info[1]
        password = login_info[2]

        return BASE_URL, username, password

    def get_url(self, remote_fname):
        data = self.conn.get_info(remote_fname)
        source_link_str = self.conn.get_source_url(remote_fname)
        return source_link_str


    def tree(self, remote_path):
        dir_info_dict = self.conn.list(remote_path)
        files_info_dict = dir_info_dict['files']
        path_list = []
        for dict_i in files_info_dict:
            path_origin = dict_i['path']
            path = path_origin.replace('cloudreve://my', '')
            f_type = dict_i['type']
            if f_type == 0: # file
                path_list.append(path)
            elif f_type == 1: # folder
                path_list_i = self.tree(path)
                path_list += path_list_i
            else:
                raise Exception('unknown file type')
        return path_list

    def get_dir_files(self, remote_path):
        dir_info_dict = self.conn.list(remote_path)
        files_info_dict = dir_info_dict['files']
        path_list = []
        for dict_i in files_info_dict:
            # pprint(dict_i)
            path = dict_i['path']
            path = path.replace('cloudreve://my', '')
            path_list.append(path)
        return path_list


    def check_is_file(self, remote_path):
        info = self.conn.get_info(remote_path)
        f_type = info['type']
        if f_type == 0: return True
        elif f_type == 1: return False
        else: raise Exception('unknown file type')


    def check_is_exists(self,remote_path):
        try:
            self.check_is_file(remote_path)
            return True
        except:
            return False

    def delete(self,remote_d):
        is_exist = self.check_is_exists(remote_d)
        if is_exist:
            try:
                self.conn.delete(remote_d)
                return True
            except:
                return False
        else:
            return True

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', default=None, help='config file path, located at ~/.config/cloudreve/')
    args = parser.parse_args()

    P = Prune(config_file=args.c)
    flist = P.get_dir_files(P.root_dir)
    for fpath in flist:
        print('deleting',fpath)
        success = P.delete(fpath)
        if success:
            print('success')
        else:
            print('failed')

    pass

if __name__ == '__main__':
    main()