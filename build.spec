# -*- mode: python ; coding: utf-8 -*-

import os
import sys
import sysconfig
from pathlib import Path

import deduce
from PyInstaller.utils.hooks import collect_all, collect_data_files, collect_submodules, copy_metadata

sys.path.insert(0, str(Path(SPECPATH) / 'app'))
from main._version import __version__

print(f'\nCore build: {__version__}\n')

# Check build OS
windows = sys.platform == 'win32'

# Update paths to match current project structure
app_path = Path(SPECPATH) / 'app'

datas = []
datas += copy_metadata('deduce')
datas += copy_metadata('fastapi')
datas += copy_metadata('uvicorn')
datas += copy_metadata('pydantic')
datas += copy_metadata('pydantic-settings')
datas += copy_metadata('polars')

datas += collect_data_files('deduce')
datas += collect_data_files('polars')
datas += collect_data_files('fastapi')
datas += collect_data_files('uvicorn')

# Add the app directory selectively
excluded_items = {
    '.vscode',
    'uv.lock',
    '.mypy_cache',
    '__pycache__',
    'data',
    'tests',
    'pytest',
    'pyproject.toml',
    'Makefile',
}

env_file = app_path / '.env'
if env_file.exists():
    datas.append((str(env_file), 'app'))

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
lookup_tables_path = app_path / 'core' / 'deduce' / 'lookup_tables'
if lookup_tables_path.exists():
    datas.append((lookup_tables_path, 'lookup_tables'))
    cache_path = lookup_tables_path
    pickle_file = cache_path / 'cache' / 'lookup_structs.pickle'

    if pickle_file.exists():
        datas.append((pickle_file, Path('lookup_tables') / 'cache'))
    else:
        deduce_instance = deduce.Deduce(lookup_data_path=lookup_tables_path, cache_path=cache_path)

binaries = []

# Windows: explicitly bundle OpenSSL DLLs required by uvicorn/ssl
if windows:
    dlls_dir = Path(sysconfig.get_paths()['stdlib']).parent / 'DLLs'
    for pattern in ['libssl*.dll', 'libcrypto*.dll']:
        for dll in dlls_dir.glob(pattern):
            binaries.append((str(dll), '.'))

hiddenimports = ['_ssl', '_hashlib']
hiddenimports += collect_submodules('deduce')
hiddenimports += collect_submodules('polars')
hiddenimports += collect_submodules('uvicorn')
hiddenimports += collect_submodules('fastapi')
hiddenimports += collect_submodules('starlette')
hiddenimports += collect_submodules('pydantic')
hiddenimports += collect_submodules('pydantic_settings')
hiddenimports += collect_submodules('anyio')

tmp_ret = collect_all('deduce')
datas += tmp_ret[0]; binaries += tmp_ret[1]; hiddenimports += tmp_ret[2]
tmp_ret = collect_all('polars')
datas += tmp_ret[0]; binaries += tmp_ret[1]; hiddenimports += tmp_ret[2]
tmp_ret = collect_all('fastapi')
datas += tmp_ret[0]; binaries += tmp_ret[1]; hiddenimports += tmp_ret[2]
tmp_ret = collect_all('uvicorn')
datas += tmp_ret[0]; binaries += tmp_ret[1]; hiddenimports += tmp_ret[2]
tmp_ret = collect_all('starlette')
datas += tmp_ret[0]; binaries += tmp_ret[1]; hiddenimports += tmp_ret[2]
tmp_ret = collect_all('pydantic')
datas += tmp_ret[0]; binaries += tmp_ret[1]; hiddenimports += tmp_ret[2]
tmp_ret = collect_all('pydantic_settings')
datas += tmp_ret[0]; binaries += tmp_ret[1]; hiddenimports += tmp_ret[2]
tmp_ret = collect_all('anyio')
datas += tmp_ret[0]; binaries += tmp_ret[1]; hiddenimports += tmp_ret[2]
tmp_ret = collect_all('fastexcel')
datas += tmp_ret[0]; binaries += tmp_ret[1]; hiddenimports += tmp_ret[2]
tmp_ret = collect_all('xlsxwriter')
datas += tmp_ret[0]; binaries += tmp_ret[1]; hiddenimports += tmp_ret[2]

a = Analysis(
    [str(app_path / 'run.py')],
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
    name='carmenda-deduce-engine',
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

coll = COLLECT(exe, a.binaries, a.datas, a.scripts, strip=False, upx=True, upx_exclude=[], name='carmenda-deduce-engine')
