from cloudreve import CloudreveV4
import urllib3
import argparse
from tqdm import tqdm
from pprint import pprint
from pathlib import Path
import os
import certifi

os.environ["SSL_CERT_FILE"] = certifi.where()
os.environ["REQUESTS_CA_BUNDLE"] = certifi.where()


class Download:

    def __init__(self):
        self.root_dir = '/_Transfer'

        BASE_URL, username, password = self.get_passwd()
        self.conn = CloudreveV4(BASE_URL)
        self.conn.login(username, password)
        print('connected to',BASE_URL)
        pass

    def get_passwd(self):
        CONFIG_FILE = Path.home() / ".config" / "cloudreve" / "passwd"
        login_info = open(CONFIG_FILE, 'r')

        login_info = login_info.read().splitlines()
        BASE_URL = login_info[0]
        username = login_info[1]
        password = login_info[2]

        return BASE_URL, username, password

        pass

    def get_url(self, remote_fname):
        data = self.conn.get_info(remote_fname)
        source_link_str = self.conn.get_source_url(remote_fname)
        return source_link_str

    def download_f(self, url, outf, chunk_size=1024 * 64, quiet=False):
        http = urllib3.PoolManager()
        r = http.request(
            "GET",
            url,
            preload_content=False
        )

        total_size = int(r.headers.get("Content-Length", 0))

        if quiet == True:
            with open(outf, "wb") as f:

                while True:
                    chunk = r.read(chunk_size)
                    if not chunk:
                        break
                    f.write(chunk)
            r.release_conn()
        else:

            with open(outf, "wb") as f, tqdm(
                total=total_size,
                unit="B",
                unit_scale=True,
                unit_divisor=1024,
                desc="Downloading",
            ) as pbar:

                while True:
                    chunk = r.read(chunk_size)
                    if not chunk:
                        break
                    f.write(chunk)
                    pbar.update(len(chunk))

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

    def download(self, remote_path, outdir='./'):
        # todo: bug in download dir on Windows
        remote_path = self.root_dir + '/' + remote_path
        if self.check_is_file(remote_path):
            path_list = [remote_path]
        else:
            path_list = self.tree(remote_path)
        for path in path_list:
            path_obj = Path(path)
            parent_dir = outdir + str(path_obj.parent).replace(self.root_dir,'')
            if not os.path.exists(parent_dir):
                os.makedirs(parent_dir)

            url = self.get_url(path)
            outf = f"{outdir}/{path.replace(self.root_dir,'')}"
            self.download_f(url, outf)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        prog="download",
        description="Download file from Cloudreve"
    )

    parser.add_argument(
        "remote_path",
        help="Remote file path in Cloudreve (e.g. xx.mp3)"
    )
    parser.add_argument(
        "local_path",
        nargs="?",
        default="./",
        help="Local path (default: current directory)"
    )

    args = parser.parse_args()
    Download().download(args.remote_path, args.local_path)