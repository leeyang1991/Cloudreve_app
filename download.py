from cloudreve import CloudreveV4
import urllib3
import argparse
from tqdm import tqdm
from pprint import pprint
from pathlib import Path
import os
import certifi
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

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
            chunk_size=5 * 1024 * 1024,  # 每个 range 块大小（5MB）
            max_workers=8,
            quiet=False
    ):
        http = urllib3.PoolManager()
        r = http.request(
            "GET",
            url,
            preload_content=False
        )

        total_size = int(r.headers.get("Content-Length", 0))

        # with open(outf, "wb") as f: # pre-allocate disk space
        #     f.truncate(total_size)

        lock = threading.Lock()

        def download_range(start, end, pbar):
            headers = {"Range": f"bytes={start}-{end}"}
            r = http.request("GET", url, headers=headers, preload_content=False)
            with open(outf, "wb") as f:
                f.seek(start)
                while True:
                    data = r.read(1024 * 64)
                    if not data:
                        break
                    f.write(data)
                    with lock:
                        pbar.update(len(data))
                        pbar.refresh()
            r.release_conn()

        # 生成 ranges
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
                disable=quiet,
                smoothing=0.1,
                mininterval=0.1
        ) as pbar, ThreadPoolExecutor(max_workers=max_workers) as executor:

            futures = [
                executor.submit(download_range, s, e, pbar)
                for s, e in ranges
            ]

            for f in as_completed(futures):
                f.result()

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

def main():
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
    pass

if __name__ == '__main__':
    main()