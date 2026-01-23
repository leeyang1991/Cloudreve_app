from cloudreve import CloudreveV4
import argparse
from tqdm import tqdm
import os
from pathlib import Path
from mimetypes import guess_type
from my_utils import Utils

login_info = open('cloudreve_passwd', 'r')
login_info = login_info.read().splitlines()
BASE_URL = login_info[0]
username = login_info[1]
password = login_info[2]
root_dir = '/rclone'


def revise_file_path(file_path: str) -> str:
    if not file_path.startswith('cloudreve://'):
        if file_path[0] != '/':
            file_path = '/' + file_path
        file_path = 'cloudreve://my' + file_path

    while file_path.endswith('//'):
        file_path = file_path[:-1]

    return file_path


class my_CloudreveV4(CloudreveV4):
    def __init__(self,
                 base_url: str = 'http://127.0.0.1:5212',
                 proxy=None,
                 verify=True,
                 headers=None,
                 cloudreve_session=None):
        super().__init__(base_url, proxy=None,
                 verify=True,
                 headers=None,
                 cloudreve_session=None)

    def upload(self, local_file_path, uri):
        '''
        上传文件
        @param local_file_path: 本地文件路径
        @param uri: 文件目标路径（包含文件名）
        '''
        local_file = Path(local_file_path)
        if not local_file.is_file():
            raise FileNotFoundError(f'{local_file_path} is not a file')

        uri = revise_file_path(uri)
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
            return self._upload_to_local(
                local_file=local_file,
                chunk_size=1024*1024,
                **r,
            )
        elif policy_type == 'onedrive':
            return self._upload_to_onedrive(
                local_file=local_file,
                **r,
            )
        else:
            raise ValueError(f'存储策略 {policy_type} 暂时不受支持')

    def _upload_to_local(self, local_file: Path, session_id, chunk_size=1024, **kwards):
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


class Upload:

    def __init__(self):
        self.conn = my_CloudreveV4(BASE_URL)
        self.conn.login(username, password)
        pass


    def upload_f(self,local_f,remote_f):
        U = Utils(self.conn)
        try:
            is_file = U.check_is_file(remote_f)
            if is_file:
                self.conn.delete(remote_f)
            else:
                self.delete_dir(remote_f)
        except:
            pass
        # exit()
        self.conn.upload(local_f,remote_f)
        # conn.upload(local_f,'/rclone/world2.nc')

    def delete_dir(self,remote_d):

        pass

    def upload_dir(self,local_d,remote_d):


        pass

def main():
    local_f = '/Users/liyang/Downloads/Prithvi-EO-2.0-main.zip'
    remote_f = '/rclone/Prithvi-EO-2.0-main.zip'
    Upload().upload_f(local_f,remote_f)
    pass

if __name__ == '__main__':
    main()