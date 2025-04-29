# -*- mode: python ; coding: utf-8 -*-

import site
import os
from PyInstaller.utils.hooks import copy_metadata, collect_data_files, collect_submodules, collect_all


site_packages = site.getsitepackages()[1]
base_config_path = os.path.join(site_packages, 'base_config.json')
rest_framework_path = os.path.join(site_packages, 'rest_framework')
drf_spectacular_path = os.path.join(site_packages, 'drf_spectacular')

datas = []
datas += copy_metadata('coreschema')
datas += copy_metadata('deduce')
datas += copy_metadata('djangorestframework')
datas += copy_metadata('drf-spectacular')

datas += collect_data_files('coreschema')
datas += collect_data_files('deduce')
datas += collect_data_files('djangorestframework')
datas += collect_data_files('rest_framework')
datas += collect_data_files('drf_spectacular')

rest_framework_imports = collect_submodules('rest_framework')
drf_spectacular_imports = collect_submodules('drf_spectacular')

datas.append((base_config_path, '.'))
datas.append((rest_framework_path, 'rest_framework'))
datas.append((drf_spectacular_path, 'drf_spectacular'))

binaries = []
hiddenimports = []
hiddenimports += collect_submodules('deduce')

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

a = Analysis(
    ['backend\\manage.py'],
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
    [],
    exclude_binaries=True,
    name='backend',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)

coll = COLLECT(
    exe,
    a.binaries,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='backend',
)
