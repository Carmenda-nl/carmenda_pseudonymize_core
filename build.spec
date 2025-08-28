# -*- mode: python ; coding: utf-8 -*-

import site
import os
import sys
import shutil
import glob
import time
from PyInstaller.utils.hooks import copy_metadata, collect_data_files, collect_submodules, collect_all

# Fix lookup tables timestamps before build to prevent cache rebuilds
def fix_lookup_timestamps():
    lookup_path = os.path.join('app', 'core', 'lookup_tables', 'src')
    cache_path = os.path.join('app', 'core', 'lookup_tables', 'cache', 'lookup_structs.pickle')
    
    if os.path.exists(lookup_path):
        # Find all .txt files in lookup tables
        txt_files = glob.glob(os.path.join(lookup_path, '**', '*.txt'), recursive=True)
        current_time = time.time()
        
        print(f"Fixing timestamps for {len(txt_files)} lookup files...")
        for txt_file in txt_files:
            os.utime(txt_file, (current_time, current_time))
        
        # If cache exists, update its timestamp to be newer than source files
        if os.path.exists(cache_path):
            # Set cache timestamp to 1 second in the future to ensure it's newer
            cache_time = current_time + 1
            os.utime(cache_path, (cache_time, cache_time))
            print("Cache timestamp updated to be newer than source files!")
        
        print("Lookup timestamps fixed!")

# Run timestamp fix before build
fix_lookup_timestamps()

# check build os
mac = sys.platform == 'darwin'
windows = sys.platform == 'win32'

if mac:
    site_packages = site.getsitepackages()[0]
elif windows:
    site_packages = site.getsitepackages()[1]
else:
    # Linux support
    site_packages = site.getsitepackages()[0]

rest_framework_path = os.path.join(site_packages, 'rest_framework') 
drf_spectacular_path = os.path.join(site_packages, 'drf_spectacular')

# Update paths to match current project structure
app_path = 'app'

datas = [] 
datas += copy_metadata('coreapi') 
datas += copy_metadata('deduce') 
datas += copy_metadata('djangorestframework') 
datas += copy_metadata('drf-spectacular')
datas += copy_metadata('polars')
datas += copy_metadata('coreschema')

datas += collect_data_files('coreapi') 
datas += collect_data_files('deduce') 
datas += collect_data_files('djangorestframework') 
datas += collect_data_files('rest_framework') 
datas += collect_data_files('drf_spectacular')
datas += collect_data_files('polars')
datas += collect_data_files('coreschema')

# Add the app directory and its contents
datas.append((app_path, 'app'))

# Add lookup_tables directory explicitly to the root for easier access
lookup_tables_path = os.path.join(app_path, 'core', 'lookup_tables')
if os.path.exists(lookup_tables_path):
    # Add the entire lookup_tables directory
    datas.append((lookup_tables_path, 'lookup_tables'))
    # Add specific important files explicitly  
    pickle_file = os.path.join(lookup_tables_path, 'cache', 'lookup_structs.pickle')
    if os.path.exists(pickle_file):
        datas.append((pickle_file, os.path.join('lookup_tables', 'cache')))

# Add the .env file explicitly
env_file = os.path.join(app_path, '.env')
if os.path.exists(env_file):
    datas.append((env_file, '.'))

rest_framework_imports = collect_submodules('rest_framework') 
drf_spectacular_imports = collect_submodules('drf_spectacular')

datas.append((rest_framework_path, 'rest_framework')) 
datas.append((drf_spectacular_path, 'drf_spectacular'))

# Add coreschema templates explicitly
coreschema_path = os.path.join(site_packages, 'coreschema')
datas.append((coreschema_path, 'coreschema'))

binaries = [] 
hiddenimports = [] 
hiddenimports += collect_submodules('deduce')
hiddenimports += collect_submodules('polars')

tmp_ret = collect_all('coreapi') 
datas += tmp_ret[0]; binaries += tmp_ret[1]; hiddenimports += tmp_ret[2] 
tmp_ret = collect_all('coreschema') 
datas += tmp_ret[0]; binaries += tmp_ret[1]; hiddenimports += tmp_ret[2]
tmp_ret = collect_all('deduce') 
datas += tmp_ret[0]; binaries += tmp_ret[1]; hiddenimports += tmp_ret[2] 
tmp_ret = collect_all('djangorestframework') 
datas += tmp_ret[0]; binaries += tmp_ret[1]; hiddenimports += tmp_ret[2] 
tmp_ret = collect_all('rest_framework') 
datas += tmp_ret[0]; binaries += tmp_ret[1]; hiddenimports += tmp_ret[2] 
tmp_ret = collect_all('drf_spectacular') 
datas += tmp_ret[0]; binaries += tmp_ret[1]; hiddenimports += tmp_ret[2]
tmp_ret = collect_all('polars') 
datas += tmp_ret[0]; binaries += tmp_ret[1]; hiddenimports += tmp_ret[2]

a = Analysis(
    [os.path.join('app', 'manage.py')],
    pathex=[],
    binaries=binaries,
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)

pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name='backend',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
