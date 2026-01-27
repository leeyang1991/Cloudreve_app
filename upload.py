## modify chunk_size in "Storage Policy"

from cloudreve import CloudreveV4
import argparse
from tqdm import tqdm
from pathlib import Path
from mimetypes import guess_type
import os
import certifi
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import zipfile

import multiprocessing
from multiprocessing.pool import ThreadPool as TPool
import copyreg
import types

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
        '''
        # 并行计算加进度条
        :param func: input a kenel_function
        :param params: para1,para2,para3... = params
        :param process: number of cpu
        :param thread_or_process: multi-thread or multi-process,'p' or 't'
        :param kwargs: tqdm kwargs
        :return:
        '''

        if process_or_thread == 'p':
            pool = multiprocessing.Pool(process)
        elif process_or_thread == 't':
            pool = TPool(process)
        else:
            raise IOError('process_or_thread key error, input keyword such as "p" or "t"')

        # results = list(tqdm(pool.imap(self.func, self.params), total=len(self.params), **kwargs))
        results = pool.imap(self.func, self.params)
        pool.close()
        pool.join()
        return results


class my_CloudreveV4(CloudreveV4):
    def __init__(self,
                 base_url: str = 'http://127.0.0.1:5212',
                 proxy=None,
                 verify=True,
                 headers=None,
                 cloudreve_session=None,
                 chunk_size=int(1024 * 1024 * 25)
                 ):
        super().__init__(base_url, proxy=None,
                 verify=True,
                 headers=None,
                 cloudreve_session=None)
        self.chunk_size = chunk_size
        self.max_workers = 5


    def upload(self, local_file_path, uri):
        '''
        上传文件
        @param local_file_path: 本地文件路径
        @param uri: 文件目标路径（包含文件名）
        '''
        local_file = Path(local_file_path)
        if not local_file.is_file():
            raise FileNotFoundError(f'{local_file_path} is not a file')
        size = local_file.stat().st_size

        if size > 1024 * 1024 * 125:  # 20MB
            upload_func = self._upload_to_local_parallel
            # print('parallel upload')
        else:
            upload_func = self._upload_to_local
            # print('serial upload')
        uri = self.revise_file_path(uri)
        dir = uri[:uri.rfind('/')]
        policy = self.list(dir)['storage_policy']
        policy_id, policy_type = policy['id'], policy['type']

        mime_type, _ = guess_type(local_file.name)
        size = local_file.stat().st_size
        time = int(local_file.stat().st_mtime * 1000)

        r = self.request('put',
                         '/file/upload',
                         json={
                             'uri': uri,
                             'size': size,
                             'last_modified': time,
                             'policy_id': policy_id,
                             'mime_type': mime_type
                         })

        if policy_type == 'remote' and r.get('upload_urls') and len(
                r['upload_urls']) > 0:
            # Remote 直传模式
            return self._upload_to_remote_direct(
                local_file=local_file,
                **r,
            )
        elif policy_type == 'local' or policy_type == 'remote':
            # Local 或 Relay 模式
            del r['chunk_size']
            return upload_func(
                local_file=local_file,
                chunk_size=self.chunk_size,
                **r,
            )
        elif policy_type == 'onedrive':
            return self._upload_to_onedrive(
                local_file=local_file,
                **r,
            )
        else:
            raise ValueError(f'存储策略 {policy_type} 暂时不受支持')

    def _upload_to_local(self, local_file, session_id, chunk_size, **kwards):
        total_size = local_file.stat().st_size

        with open(local_file, 'rb') as file, tqdm(
                total=total_size,
                unit='B',
                unit_scale=True,
                unit_divisor=1024,
                desc=f'Uploading {local_file.name}',
        ) as pbar:

            block_id = 0
            while True:
                chunk = file.read(chunk_size)
                if not chunk:
                    break

                self.request(
                    'post',
                    f'/file/upload/{session_id}/{block_id}',
                    headers={
                        'Content-Length': str(len(chunk)),
                        'Content-Type': 'application/octet-stream',
                    },
                    data=chunk,
                )

                block_id += 1
                pbar.update(len(chunk))

    def _upload_to_local_parallel1(
            self,
            local_file,
            session_id,
            chunk_size,
            **kwards
    ):
        total_size = local_file.stat().st_size
        lock = threading.Lock()

        def upload_block(block_id, data):
            self.request(
                'post',
                f'/file/upload/{session_id}/{block_id}',
                headers={
                    'Content-Length': str(len(data)),
                    'Content-Type': 'application/octet-stream',
                },
                data=data,
            )
            with lock:
                pbar.update(len(data))

        MAX_INFLIGHT = self.max_workers * 2
        with open(local_file, 'rb') as f, tqdm(
                total=total_size,
                unit='B',
                unit_scale=True,
                unit_divisor=1024,
                desc=f'Uploading {local_file.name}',
        ) as pbar, ThreadPoolExecutor(max_workers=self.max_workers) as executor:

            # futures = []
            block_id = 0
            futures = {}
            while True:
                data = f.read(chunk_size)
                if not data:
                    break
                future = executor.submit(upload_block, block_id, data)
                futures[future] = block_id
                block_id += 1

                if len(futures) >= MAX_INFLIGHT:
                    for done in as_completed(futures):
                        done.result()  # 抛异常
                        futures.pop(done)
                        break
            for future in as_completed(futures):
                future.result()

    def _upload_to_local_parallel(
            self,
            local_file,
            session_id,
            chunk_size,
            **kwards
    ):
        total_size = local_file.stat().st_size
        njob = self.max_workers
        if len(local_file.name) > 36:
            desc_name = local_file.name[:36] + '...'
        else:
            desc_name = local_file.name
        with open(local_file, 'rb') as f, tqdm(
                total=total_size,
                unit='B',
                unit_scale=True,
                unit_divisor=1024,
                desc=f'Uploading {desc_name}',
        ) as pbar:

            block_id = 0
            BATCH = njob * 2
            params_list = []
            while True:
                data = f.read(chunk_size)
                if not data:
                    break
                params = block_id, data,session_id, pbar
                block_id += 1
                params_list.append(params)
                if len(params_list) >= BATCH:
                    MULTIPROCESS(self.kernel_upload_block,params_list).run(process=njob,process_or_thread='t')
                    params_list = []
            if len(params_list) > 0:
                MULTIPROCESS(self.kernel_upload_block, params_list).run(process=len(params_list), process_or_thread='t')


    def kernel_upload_block(self,params):
        block_id, data,session_id,pbar = params
        self.request(
            'post',
            f'/file/upload/{session_id}/{block_id}',
            headers={
                'Content-Length': str(len(data)),
                'Content-Type': 'application/octet-stream',
            },
            data=data,
        )
        pbar.update(len(data))

    def get_url(self,remote_fname):
        data = self.get_info(remote_fname)
        source_link_str = self.get_source_url(remote_fname)
        return source_link_str

    def revise_file_path(self, file_path: str) -> str:
        if not file_path.startswith('cloudreve://'):
            if file_path[0] != '/':
                file_path = '/' + file_path
            file_path = 'cloudreve://my' + file_path

        while file_path.endswith('//'):
            file_path = file_path[:-1]

        return file_path

class Utils_cloudreve:
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

    def check_is_exists(self,remote_path):
        try:
            self.check_is_file(remote_path)
            return True
        except:
            return False


class Upload:

    def __init__(self):
        BASE_URL, username, password = self.get_passwd()
        self.conn = my_CloudreveV4(BASE_URL)
        self.conn.login(username, password)
        print('connected to', BASE_URL)
        self.Util = Utils_cloudreve(self.conn)
        self.root_dir = '/_Transfer'
        self.conn.create_dir(self.root_dir)
        pass

    def get_passwd(self):
        CONFIG_FILE = Path.home() / ".config" / "cloudreve" / "passwd"
        login_info = open(CONFIG_FILE, 'r')

        login_info = login_info.read().splitlines()
        BASE_URL = login_info[0]
        username = login_info[1]
        password = login_info[2]

        return BASE_URL, username, password


    def upload_f(self,local_f,remote_f=None,overwrite=True):

        path_obj = Path(local_f)
        if remote_f is None:
            remote_f = self.root_dir + '/' + str(path_obj.name)
        # print(remote_f)
        remote_f_obj = Path(remote_f)
        parent = str(remote_f_obj.parent)
        suffix = str(remote_f_obj.suffix)
        prefix = str(remote_f_obj.name.replace(suffix, ''))
        if overwrite:
            is_available = self.delete(remote_f)
            if not is_available:
                remote_f = parent + '/' + prefix + '(new)' + suffix
                print(remote_f)
                self.upload_f(local_f,remote_f,overwrite)
            else:
                self.conn.upload(local_f,remote_f)
        else:
            is_exist = self.Util.check_is_exists(remote_f)
            if is_exist:
                print(f'{remote_f} already exists')
                return
            else:
                self.conn.upload(local_f,remote_f)

    def delete(self,remote_d):
        is_exist = self.Util.check_is_exists(remote_d)
        if is_exist:
            try:
                self.conn.delete(remote_d)
                return True
            except:
                return False
        else:
            return True

    def mkdir(self,remote_d):
        self.conn.create_dir(remote_d)

    def upload_dir(self,local_d,remote_d=None,overwrite=True):
        path_obj = Path(local_d)
        if remote_d is None:
            remote_d = self.root_dir + '/' + str(path_obj.name)
        if overwrite:
            is_available = self.delete(remote_d)
            if not is_available:
                remote_d = remote_d + '(new)'
                self.upload_dir(local_d,remote_d,overwrite)
            for root, dirs, files in os.walk(local_d):
                files = sorted(files)
                for file in files:
                    if file.startswith('.'):
                        continue
                    local_f = os.path.join(root, file)
                    remote_d_i = remote_d + '/' + str(root.replace(local_d,''))
                    remote_f = remote_d_i + '/' + file
                    self.mkdir(remote_d_i)
                    self.upload_f(local_f,remote_f,overwrite)
        else:
            for root, dirs, files in os.walk(local_d):
                files = sorted(files)
                for file in files:
                    if file.startswith('.'):
                        continue
                    local_f = os.path.join(root, file)
                    remote_d_i = remote_d + '/' + str(root.replace(local_d,''))
                    remote_f = remote_d_i + '/' + file
                    self.mkdir(remote_d_i)
                    self.upload_f(local_f,remote_f,overwrite)
        pass

def zip_file(src: Path, dst: Path = None) -> Path:
    """
    压缩单个文件
    :param src: 要压缩的文件路径
    :param dst: 输出 zip 路径（可选，默认同目录）
    :return: zip 文件路径
    """
    src = Path(src).resolve()
    if not src.is_file():
        raise ValueError(f"{src} is not a file")

    if dst is None:
        dst = src.with_suffix(src.suffix + ".zip")
    else:
        dst = Path(dst).resolve()
    print('Compressing',src.name)
    with zipfile.ZipFile(dst, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.write(src, arcname=src.name)

    return dst


def zip_dir(src_dir: Path, dst: Path = None) -> Path:
    """
    压缩整个文件夹（带进度条，按文件数）
    """
    src_dir = Path(src_dir).resolve()
    if not src_dir.is_dir():
        raise ValueError(f"{src_dir} is not a directory")

    if dst is None:
        dst = src_dir.with_suffix(".zip")
    else:
        dst = Path(dst).resolve()

    files = [p for p in src_dir.rglob("*") if p.is_file()]

    with zipfile.ZipFile(dst, "w", zipfile.ZIP_DEFLATED) as zf, tqdm(
        total=len(files),
        desc=f"Compressing {src_dir.name}",
        unit="file",
    ) as pbar:

        for p in files:
            zf.write(p, arcname=p.relative_to(src_dir.parent))
            pbar.update(1)

    return dst

def upload(path,iszip=True,overwrite=True):
    if iszip:
        path = Path(path)
        if os.path.isdir(path):
            dst = zip_dir(path)
        elif os.path.isfile(path):
            dst = zip_file(path)
        else:
            raise Exception(f'{path} not exist')
        Upload_obj = Upload()
        Upload_obj.upload_f(dst,overwrite=overwrite)
        os.remove(dst)

    else:
        Upload_obj = Upload()
        if os.path.isdir(path):
            Upload_obj.upload_dir(path,overwrite=overwrite)
        elif os.path.isfile(path):
            Upload_obj.upload_f(path,overwrite=overwrite)
        else:
            raise Exception(f'{path} not exist')
    pass

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('path', help='Local file path')
    parser.add_argument('--nozip', action='store_false', help='disable zip')
    parser.add_argument('--no-overwrite', action='store_false', help='overwrite existing file')
    args = parser.parse_args()
    upload(args.path, args.nozip,args.no_overwrite)

if __name__ == '__main__':
    main()