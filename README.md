# Carmenda privacy tool

![Linter](https://img.shields.io/badge/linter-ruff-4B8B3B)

**Carmenda privacy tool** is a solution designed to pseudonymize textual data for care organizations.  
This tool leverages the **[Deduce](https://github.com/vmenger/deduce)** tool (Menger et al. 2017) [1]
algorithm to effectively mask sensitive information, ensuring compliance with data privacy regulations.  
Built with **Polars** for enhanced performance, it is ideal for efficiently handling large datasets.

## Features

- **Pseudonymization of Textual Data**: Masks sensitive information using the Deduce algorithm.
- **High Performance**: Utilizes Polars to process large datasets quickly and efficiently.
- **Easy Deployment**: Provided as a Docker image for simple setup and deployment.

## Getting Started

Follow the instructions on the [wiki](https://github.com/Carmenda-nl/Carmenda_pseudonymize/wiki).

## How It Works

Carmenda privacy tool uses the Deduce algorithm to replace sensitive information in textual data with pseudonyms.  
This method ensures that the data remains useful for analytical purposes while safeguarding individual privacy.

## License

This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later.  
© 2025 Carmenda. All rights reserved.

## Contact

For questions or support, please contact us at [support@carmenda.nl](mailto:support@carmenda.nl).

---

## Docker Deployment (Core)

### Build the Docker Image

Build the image from the Dockerfile in the main folder (works on Linux, macOS and Windows):

```bash
# Run this command from the project root directory
docker build -f deployment/Dockerfile -t privacy-core:latest .
```

### Show Help

To see the available command-line options:

```bash
docker run --rm privacy-core:latest --help
```

### Run the Privacy tool

Use the following command to run the privacy tool with mounted volumes:

```bash
docker run --rm \
  -v /path/to/your/input:/app/data/input \
  -v /path/to/your/output:/app/data/output \
  privacy-core \
  --input_fofi /app/data/input/your_file.csv \
  --output_extension .csv
```

**Example for Linux/macOS:**

```bash
docker run --rm \
  -v /Users/h.j.m.tummers/docker_test/data/input:/app/data/input \
  -v /Users/h.j.m.tummers/docker_test/data/output:/app/data/output \
  privacy-core \
  --input_fofi /app/data/input/test_SHL.csv \
  --output_extension .csv
```

**Example for Windows (PowerShell):**

```powershell
docker run --rm `
  -v C:\Users\username\docker_test\data\input:/app/data/input `
  -v C:\Users\username\docker_test\data\output:/app/data/output `
  privacy-core `
  --input_fofi /app/data/input/test_SHL.csv `
  --output_extension .csv
```

**Example for Windows (Command Prompt):**

```cmd
docker run --rm ^
  -v C:\Users\username\docker_test\data\input:/app/data/input ^
  -v C:\Users\username\docker_test\data\output:/app/data/output ^
  privacy-core ^
  --input_fofi /app/data/input/test_SHL.csv ^
  --output_extension .csv
```

### Additional Options

For custom column mappings:

```bash
docker run --rm \
  -v /path/to/your/input:/app/data/input \
  -v /path/to/your/output:/app/data/output \
  privacy-core \
  --input_fofi /app/data/input/your_file.csv \
  --input_cols "patientName=Cliëntnaam, report=rapport" \
  --output_cols "patientID=patientID, processed_report=processed_report" \
  --output_extension .csv
```

### Important Notes

- **Input Path**: Always use `/app/data/input/` as the base path for input files in the container
- **Output Path**: Always use `/app/data/output/` as the base path for output files in the container
- **Volume Mounting**:
  - The `-v` flags map your local directories to the container directories
  - On Windows, use Windows paths (C:\path\to\folder) on the left side of the colon, but Unix-style paths on the right side
  - Note: The paths inside the container always remain Unix-style (/app/data/...)
- **File Extensions**: Supported formats are `.csv` and `.parquet`
- **Remove Container**: The `--rm` flag automatically removes the container after execution

### Troubleshooting

If you don't see output files:

1. Check that the volume mounts are correct
2. Verify that the input file path starts with `/app/data/input/`
3. Ensure the output directory has write permissions
4. Check the container logs for any error messages

**Windows-specific tips:**

1. Use absolute paths for volume mounts, relative paths may not work well on Windows
2. If you have issues accessing volumes, try configuring Docker Desktop for file sharing
3. Docker Desktop needs access to the folders you want to mount (see Docker Desktop settings)

[1] Menger, V.J., Scheepers, F., van Wijk, L.M., Spruit, M. (2017). DEDUCE: A pattern matching method for automatic de-identification of Dutch medical text, Telematics and Informatics, 2017, ISSN 0736-5853

---

## Backend Development

### Step 1: Preparations

Ensure Python is installed (minimum version 3.10) and Git.
Clone this repository to your local machine.

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

### Step 2: Install dependencies

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

## Backend Building

### Step 1: Setup

Pull this repository to your local machine.

```bash
git clone --recursive https://github.com/Carmenda-nl/carmenda_pseudonymize_backend.git
```

> **Important:** The operating system (OS) you use will determine the target build.  
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

## Docker Build (Backend)

```bash
docker build -t backend .
```

```bash
docker run -p 8000:8000 backend
```
