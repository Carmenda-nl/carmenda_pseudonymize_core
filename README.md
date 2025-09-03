# Carmenda Privacy Tool - Backend API

![License](https://img.shields.io/badge/license-GPL--3.0--or--later-blue)
![Python](https://img.shields.io/badge/python-3.13.6-blue)
![Django](https://img.shields.io/badge/django-5.2.5-green)
![Linter](https://img.shields.io/badge/linter-ruff-4B8B3B)
![Docker](https://img.shields.io/badge/docker-enabled-blue)
![API](https://img.shields.io/badge/API-REST-orange)
![Documentation](https://img.shields.io/badge/docs-swagger-85EA2D)
![Privacy](https://img.shields.io/badge/privacy-GDPR%20compliant-green)
![Healthcare](https://img.shields.io/badge/healthcare-dutch%20medical-red)

**Carmenda privacy tool** is a REST API solution designed to pseudonymize textual data for care organizations.  
This backend leverages the **[Deduce](https://github.com/vmenger/deduce)** tool (Menger et al. 2017) [1]
algorithm to effectively mask sensitive information, ensuring compliance with data privacy regulations.  
Built with **Polars** for enhanced performance, it provides a scalable API for handling large datasets.

[1] Menger, V.J., Scheepers, F., van Wijk, L.M., Spruit, M. (2017). DEDUCE: A pattern matching method for automatic
de-identification of Dutch medical text, Telematics and Informatics, 2017, ISSN 0736-5853

## Features

- **REST API for Pseudonymization**: HTTP endpoints for text de-identification using the Deduce algorithm
- **High Performance**: Utilizes Polars to process large datasets quickly and efficiently
- **API Documentation**: Automatic OpenAPI/Swagger documentation for easy integration
- **Docker Deployment**: Provided as a Docker image for simple setup and deployment

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

## Docker Deployment (Backend)

### Build the Docker Image

Build the backend image from the Dockerfile:

```bash
# Run this command from the project root directory
docker build -f deployment/Dockerfile -t privacy-backend:latest .
```

### Run the Privacy tool

Use the following command to run the privacy tool with mounted volumes:

```bash
docker run --rm -p 8000:8000 -e DEBUG=False privacy-backend:latest
```

The API will be available at `http://localhost:8000/`

### API Documentation

Once the backend is running, you can access:

- **API Documentation**: `http://localhost:8000/docs/` (Swagger UI)
- **API Schema**: `http://localhost:8000/schema/` (OpenAPI schema)

### Important Notes

- **Port**: The backend runs on port 8000 by default
- **API Endpoints**: All API endpoints are documented at `/docs/` when the server is running
- **Logs**: Container logs will show Django server output and any errors in the terminal

### Troubleshooting

If you encounter issues:

1. Check that port 8000 is not already in use
2. Verify that Docker has sufficient resources allocated

---

## Backend Development

### Step 1: Preparations

Ensure Python is installed (minimum version 3.10) and Git.
Clone this repository to your local machine.

```bash
git clone --recursive https://github.com/Carmenda-nl/carmenda_pseudonymize_backend.git
```

Open a terminal and navigate to the app folder of this project:

```bash
cd app
```

Create a virtual environment, as all dependencies need to be loaded into it:

```bash
python virtualenv .venv
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
DEBUG=True
SECRET_KEY=add any string-based key here
CSRF_TRUSTED_ORIGINS=http://127.0.0.1
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

Open a terminal and navigate to the app folder:

```bash
cd app
```

Create a virtual environment, as all dependencies need to be loaded into it:

```bash
virtualenv .venv
```

Activate the virtual environment:

```bash
.venv\Scripts\activate
```

> **Note:** This example assumes you are on a Windows system *without* using WSL.  
> **Be aware:** Using WSL will create a Linux-based build.

Install the project dependencies:

```bash
pip install -r requirements.txt
```

Add a `.env` file with the following data

```bash
DJANGO_RUNSERVER_HIDE_WARNING=true
DEBUG=False
SECRET_KEY=add any string-based key here
CSRF_TRUSTED_ORIGINS=http://127.0.0.1
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
cd dist\backend\
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

If everything is functioning correctly, you can copy the `backend` folder from
the dist folder to the frontend dist folder.
