# -*- mode: python ; coding: utf-8 -*-

import os
import site
import subprocess
import sys
from pathlib import Path

import deduce
from PyInstaller.utils.hooks import collect_all, collect_data_files, collect_submodules, copy_metadata

# Write the latest git tag to a version file
version_file = Path(SPECPATH) / 'app' / '_version.txt'
version = (
    subprocess.check_output(
        ['git', 'describe', '--tags', '--abbrev=0'],
        cwd=SPECPATH,
        stderr=subprocess.DEVNULL,
    )
    .decode('utf-8')
    .strip()
)

with version_file.open('w', encoding='utf-8') as file:
    file.write(version)

print(f'\nCore build version: {version}\n')

# Check build OS
windows = sys.platform == 'win32'
site_packages = site.getsitepackages()[1] if windows else site.getsitepackages()[0]

rest_framework_path = Path(site_packages) / 'rest_framework'
drf_spectacular_path = Path(site_packages) / 'drf_spectacular'

# Update paths to match current project structure
app_path = Path(SPECPATH) / 'app'

datas = []
datas += copy_metadata('deduce')
datas += copy_metadata('drf-spectacular')
datas += copy_metadata('polars')

# Ensure daphne/autobahn (and their native extensions) are collected by PyInstaller
datas += copy_metadata('daphne')
datas += copy_metadata('autobahn')
datas += copy_metadata('twisted')

datas += collect_data_files('deduce')
datas += collect_data_files('rest_framework')
datas += collect_data_files('drf_spectacular')
datas += collect_data_files('polars')
datas += collect_data_files('daphne')
datas += collect_data_files('autobahn')
datas += collect_data_files('twisted')

# Add the app directory selectively
excluded_items = {
    '.vscode',
    'uv.lock',
    '.mypy_cache',
    '__pycache__',
    'data',
    'tests',
    'pytest',
    'core.py',
    'pyproject.toml',
}

for root, dirs, files in os.walk(app_path):
    dirs[:] = [directory for directory in dirs if directory not in excluded_items and not directory.startswith('.')]

    for filename in files:
        if isinstance(filename, str) and filename not in excluded_items and not filename.startswith('.'):
            source_path = str(Path(root) / filename)
            rel_path = os.path.relpath(root, app_path)

            dest_path = str(Path('app') / rel_path) if rel_path != '.' else 'app'
            datas.append((source_path, dest_path))

# Filter out files and folders not needed for production
datas = [
    (source, dest)
    for source, dest in datas
    if not (isinstance(source, str) and ('__pycache__' in source or '.pyc' in source))
]

# Bundle the lookup tables in the application
lookup_tables_path = app_path / 'core' / 'lookup_tables'
if lookup_tables_path.exists():
    datas.append((lookup_tables_path, 'lookup_tables'))
    cache_path = lookup_tables_path
    pickle_file = cache_path / 'cache' / 'lookup_structs.pickle'

    if pickle_file.exists():
        datas.append((pickle_file, Path('lookup_tables') / 'cache'))
    else:
        deduce_instance = deduce.Deduce(lookup_data_path=lookup_tables_path, cache_path=cache_path)

rest_framework_imports = collect_submodules('rest_framework')
drf_spectacular_imports = collect_submodules('drf_spectacular')

datas.append((str(rest_framework_path), 'rest_framework'))
datas.append((str(drf_spectacular_path), 'drf_spectacular'))

binaries = []
hiddenimports = []
hiddenimports += collect_submodules('deduce')
hiddenimports += collect_submodules('polars')
hiddenimports += collect_submodules('daphne')
hiddenimports += collect_submodules('autobahn')
hiddenimports += collect_submodules('twisted')

tmp_ret = collect_all('deduce')
datas += tmp_ret[0]; binaries += tmp_ret[1]; hiddenimports += tmp_ret[2]
tmp_ret = collect_all('rest_framework')
datas += tmp_ret[0]; binaries += tmp_ret[1]; hiddenimports += tmp_ret[2]
tmp_ret = collect_all('drf_spectacular')
datas += tmp_ret[0]; binaries += tmp_ret[1]; hiddenimports += tmp_ret[2]
tmp_ret = collect_all('polars')
datas += tmp_ret[0]; binaries += tmp_ret[1]; hiddenimports += tmp_ret[2]
tmp_ret = collect_all('daphne')
datas += tmp_ret[0]; binaries += tmp_ret[1]; hiddenimports += tmp_ret[2]
tmp_ret = collect_all('autobahn')
datas += tmp_ret[0]; binaries += tmp_ret[1]; hiddenimports += tmp_ret[2]
tmp_ret = collect_all('twisted')
datas += tmp_ret[0]; binaries += tmp_ret[1]; hiddenimports += tmp_ret[2]

a = Analysis(
    [str(app_path / 'manage.py')],
    pathex=[],
    binaries=binaries,
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[
        'pytest',
        'test',
        'tests',
        'hypothesis',
        'IPython',
        'jupyter',
        'notebook',
        'tkinter',
        'Tkinter',
        'pdb',
        'matplotlib',
        'pylab',
    ],
    noarchive=False,
    optimize=1,
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
    strip=True,
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

coll = COLLECT(exe, a.binaries, a.datas, a.scripts, strip=False, upx=True, upx_exclude=[], name='backend')
