# CARMENDA PSEUDOMIZE APP - Backend Building

This project uses a PyInstaller build backend,
as the backend does not require any Python dependencies at runtime and is multiplatform.  
The following guide explains the building process.

## Step 1: Preparations

Pull this repository to your local machine.  
> Important:** The operating system (OS) you use will determine the target build.  
> For example, using Windows will generate a Windows executable; using Linux will generate a Linux executable.

Ensure Python is installed (minimum version 3.10).

Open a terminal and navigate to the backend folder:

```bash
cd code\backend
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

## Step 2: Building

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
pyinstaller backend.spec --noconfirm
```

> This process may take some time.

After building is complete, navigate to the created distribution folder:

```bash
cd dist\backend
```

Test-run the built backend:

```bash
backend.exe runserver --noreload
```

If everything is functioning correctly, you can copy the `dist` folder along
with the `.exe` and `_internal` folder to deploy the backend.

## Docker build

```bash
docker build -t backend .
```

docker run -p 8000:8000 backend

Django version 5.1.7, using settings 'main.settings'
Starting development server at http://0.0.0.0:8000/
Quit the server with CONTROL-C.

127.0.0.1