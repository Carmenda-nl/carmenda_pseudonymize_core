# -*- mode: python ; coding: utf-8 -*-

import site
import os
import sys
from PyInstaller.utils.hooks import copy_metadata, collect_data_files, collect_submodules, collect_all


# Check build OS
windows = sys.platform == 'win32'

if windows:
    site_packages = site.getsitepackages()[1]
else:
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

# Bundle the lookup tables in the application
lookup_tables_path = os.path.join(app_path, 'core', 'lookup_tables')
if os.path.exists(lookup_tables_path):
    datas.append((lookup_tables_path, 'lookup_tables'))
    pickle_file = os.path.join(lookup_tables_path, 'cache', 'lookup_structs.pickle')

    if os.path.exists(pickle_file):
        datas.append((pickle_file, os.path.join('lookup_tables', 'cache')))

# Add the .env file if available
env_file = os.path.join(app_path, '.env')
if os.path.exists(env_file):
    datas.append((env_file, '.'))

rest_framework_imports = collect_submodules('rest_framework') 
drf_spectacular_imports = collect_submodules('drf_spectacular')

datas.append((rest_framework_path, 'rest_framework')) 
datas.append((drf_spectacular_path, 'drf_spectacular'))

# Add coreschema templates
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
