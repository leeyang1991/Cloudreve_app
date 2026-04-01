conda activate cloudreve
pyinstaller --onefile upload.py && pyinstaller --onefile  download.py && pyinstaller --onefile prune.py
zip -r cloudreve.zip dist/