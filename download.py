from cloudreve import CloudreveV4
import urllib3
import argparse
from tqdm import tqdm
from pprint import pprint
from pathlib import Path
import os
import certifi

import multiprocessing
from multiprocessing.pool import ThreadPool as TPool
import copyreg
import types

import sys


os.environ["SSL_CERT_FILE"] = certifi.where()
os.environ["REQUESTS_CA_BUNDLE"] = certifi.where()

class MULTIPROCESS:

    def __init__(self, func, params):
        self.func = func
        self.params = params
        copyreg.pickle(types.MethodType, self._pickle_method)
        pass

    def _pickle_method(self, m):
        if m.__self__ is None:
            return getattr, (m.__self__.__class__, m.__func__.__name__)
        else:
            return getattr, (m.__self__, m.__func__.__name__)

    def run(self, process=4, process_or_thread='p', **kwargs):

        if process_or_thread == 'p':
            pool = multiprocessing.Pool(process)
        elif process_or_thread == 't':
            pool = TPool(process)
        else:
            raise IOError('process_or_thread key error, input keyword such as "p" or "t"')

        results = pool.imap(self.func, self.params)
        pool.close()
        pool.join()
        return results


class Download:

    def __init__(self, config_file=None):
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
                sys.exit(0)
            else:
                print(f'config file not found: {self.config_file}')
                print('please use -c to select a config file')

                print('config options:',' | '.join(config_file_list))
                sys.exit(0)

        BASE_URL, username, password, token = self.get_passwd()
        self.conn = CloudreveV4(BASE_URL)
        if token is None:
            self.conn.login(username, password)
        else:
            self.conn.session.headers.update({'Authorization': 'Bearer ' + token})
        print('connected to',BASE_URL)

        pass

    def get_passwd(self):
        CONFIG_FILE = self.config_file
        login_info = open(CONFIG_FILE, 'r')

        login_info = login_info.read().splitlines()
        BASE_URL = login_info[0]
        username = login_info[1]
        password = login_info[2]
        if len(login_info) > 3:
            token = login_info[3]
        else:
            token = None
        return BASE_URL, username, password, token

    def get_url(self, remote_fname):
        data = self.conn.get_info(remote_fname)
        source_link_str = self.conn.get_source_url(remote_fname)
        return source_link_str

    def download_f_single(self, url, outf, chunk_size=1024 * 64, quiet=False):
        http = urllib3.PoolManager()
        r = http.request(
            "GET",
            url,
            preload_content=False
        )

        total_size = int(r.headers.get("Content-Length", 0))

        with open(outf, "wb") as f, tqdm(
            total=total_size,
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
            desc="Downloading",
            smoothing=0.1
        ) as pbar:

            while True:
                chunk = r.read(chunk_size)
                if not chunk:
                    break
                f.write(chunk)
                pbar.update(len(chunk))

        r.release_conn()

    def download_f_parallel(
            self,
            url,
            outf,
            chunk_size=10 * 1024 * 1024,  # 每个 range 块大小（10MB）
    ):
        http_conn = urllib3.PoolManager()
        r = http_conn.request(
            "GET",
            url,
            preload_content=False
        )

        total_size = int(r.headers.get("Content-Length", 0))

        if total_size > 1024*1024*300:
            print('pre-allocating disk space ...')

        with open(outf, "wb") as f: # pre-allocate disk space
            f.truncate(total_size)

        ranges = []
        for start in range(0, total_size, chunk_size):
            end = min(start + chunk_size - 1, total_size - 1)
            ranges.append((start, end))

        with tqdm(
                total=total_size,
                unit="B",
                unit_scale=True,
                unit_divisor=1024,
                desc="Downloading",
                smoothing=0.1,
        ) as pbar:
            params_list = []
            for start, end in ranges:
                params = start, end, pbar,r,url,outf,http_conn
                params_list.append(params)
                # self.download_range(params)
            max_workers = min(8, len(params_list))
            MULTIPROCESS(self.download_range,params_list).run(process=max_workers,process_or_thread='t')


    def download_range(self,params):
        start, end, pbar, r, url, outf, http_conn = params
        headers = {"Range": f"bytes={start}-{end}"}
        r = http_conn.request("GET", url, headers=headers, preload_content=False)
        with open(outf, "r+b") as f:
            f.seek(start)
            while True:
                data = r.read(1024 * 64)
                if not data:
                    break
                f.write(data)
                pbar.update(len(data))
        r.release_conn()

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
            pprint(dict_i)
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

    def download(self, remote_path, outdir:str=None):
        if outdir == None:
            outdir = os.getcwd()
        outdir = Path(outdir)
        remote_path = self.root_dir + '/' + remote_path
        if self.check_is_file(remote_path):
            path_list = [remote_path]
        else:
            path_list = self.tree(remote_path)
        for path_remote in path_list:
            path_local = path_remote.replace(self.root_dir+'/', '')
            outf = outdir / path_local
            parent_dir = outf.parent
            parent_dir.mkdir(parents=True, exist_ok=True)
            url = self.get_url(path_remote)
            self.download_f_parallel(url, outf)
            # self.download_f1(url, outf)

def download(remote_path,outdir=None,config_file=None):
    Download(config_file=config_file).download(remote_path=remote_path, outdir=outdir)

    pass

def main():
    config_dir = Path.home() / ".config" / "cloudreve"

    parser = argparse.ArgumentParser(
        prog="download",
        description="Download file from Cloudreve"
    )

    parser.add_argument(
        "remote_path",
         nargs = '*',
        help="Remote file path in Cloudreve (e.g. xx.mp3)"
    )
    parser.add_argument('-folder', default=None, help=f'local folder to save downloaded files, default is current directory')
    parser.add_argument('-c', default=None, help=f'config file path, located at {config_dir}, default is "passwd"')

    parser.add_argument('-ls', action='store_true', help=f'list config files in {config_dir}')

    args = parser.parse_args()

    if args.ls:
        config_file_list = sorted(os.listdir(Path.home() / ".config" / "cloudreve"))
        print('config options:', ' | '.join(config_file_list))
        sys.exit(0)

    args = parser.parse_args()

    if len(args.remote_path) == 0:
        parser.print_help()
        sys.exit(0)
    else:
        remote_path = args.remote_path[0]
    download(remote_path=remote_path, outdir=args.folder, config_file=args.c)
    pass

if __name__ == '__main__':
    main()