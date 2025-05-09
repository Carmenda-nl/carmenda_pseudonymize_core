# CARMENDA PSEUDOMIZE APP

This project uses a PyInstaller build backend,
as the backend does not require any Python dependencies at runtime and is multiplatform.  
The following guide explains: How to setup a development or building environment locally.

## Development

### Step 1: Preparations

Ensure Python is installed (minimum version 3.10) and Git.
Clonel this repository to your local machine.

```bash
git clone --recursive https://github.com/Carmenda-nl/carmenda_pseudonymize_backend.git
```

Open a terminal and navigate to the code folder of this project:

```bash
cd code
```

Generate 3 symlinks (symbolic links) to the external core.

```bash
ln -s ../../external/core/app/polars_deduce.py code/services/polars_deduce.py
ln -s ../../external/core/app/logger.py code/services/logger.py
ln -s ../../external/core/app/progress_tracker.py code/services/progress_tracker.py
```

> Only possible on Linux based dev environments.
> If on windows, copy the 3 files from the `external/core/apps` to `code/services` **WSL** is advisable for development

Create a virtual environment, as all dependencies need to be loaded into it:

```bash
virtualenv .venv
```

Activate the virtual environment:

```bash
source .venv\bin\activate
```

> **Note:** This example assumes you are on a Windows system using WSL.  

### Step 2: Install dependendies

Install the project dependencies:

```bash
pip install -r requirements.txt
```

Add a `.env` file with the following data

```bash
SECRET_KEY=add any key here
DEBUG=False
```

You can now test-run the server to verify everything functions properly:

```bash
python manage.py runserver
```

## Backend building

### Step 1: Setup

Pull this repository to your local machine.

```bash
git clone --recursive https://github.com/Carmenda-nl/carmenda_pseudonymize_backend.git
```

> Important:** The operating system (OS) you use will determine the target build.  
> For example, using Windows will generate a Windows executable; using Mac will generate a MacOS executable.

Ensure Python is installed (minimum version 3.10).

Open a terminal and navigate to the code folder:

```bash
cd code
```

Create a virtual environment, as all dependencies need to be loaded into it:

```bash
virtualenv .venv
```

Activate the virtual environment:

```bash
.venv\Scripts\activate
```

> **Note:** This example assumes you are on a Windows system without using WSL.  
> **Be aware:** Using WSL will create a Linux-based build.

Install the project dependencies:

```bash
pip install -r requirements.txt
```

You can now test-run the server to verify everything functions properly:

```bash
python manage.py runserver
```

### Step 2: Building

With the terminal still open and the virtual environment active, move up one folder:

```bash
cd ..
```

Install PyInstaller:

```bash
pip install pyinstaller
```

Build the backend with the following command:

```bash
pyinstaller build.spec --noconfirm
```

> This process may take some time.

### Step 3: Run

After building is complete, navigate to the created distribution folder:

```bash
cd dist\backend
```

Test-run the built backend:

On windows this is:

```bash
backend.exe runserver --noreload
```

On a mac run:

```bash
./backend runserver --noreload
```

If everything is functioning correctly, you can copy the `backend` file from
the dist folder to the frontend dist.

---

## Docker build

```bash
docker build -t backend .
```

```bash
docker run -p 8000:8000 backend
```
