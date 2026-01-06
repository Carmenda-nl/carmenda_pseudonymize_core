# Carmenda Privacytool - Backend API

[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![Python](https://img.shields.io/badge/python-3.13-blue)](https://www.python.org/downloads/release/python-3136/)
[![Django](https://img.shields.io/badge/django-5.2.8-green)](https://docs.djangoproject.com/en/5.2/)
[![Polars](https://img.shields.io/badge/polars-1.35-9cf)](https://pola.rs/)
[![API](https://img.shields.io/badge/api-REST-orange)](https://www.django-rest-framework.org/)
[![License](https://img.shields.io/badge/license-GPL--3.0-blue)](https://www.gnu.org/licenses/gpl-3.0.html)

**Carmenda Privacytool** is a REST API solution designed to pseudonymize textual data for care organizations.

This backend leverages the **[Deduce](https://github.com/vmenger/deduce)** algorithm **[1]**
to effectively mask sensitive information, ensuring compliance with data privacy regulations.  
Built with **Polars** for enhanced performance, it provides a scalable API for handling large datasets efficiently.

**[1]** _Menger, V.J., Scheepers, F., van Wijk, L.M., Spruit, M. (2017). DEDUCE: A pattern matching method for automatic
de-identification of Dutch medical text, Telematics and Informatics, 2017, ISSN 0736-5853_

## Features

- **REST API for Pseudonymization**: HTTP endpoints for text de-identification using the Deduce algorithm
- **High Performance**: Utilizes Polars with vectorized operations to process large datasets quickly and efficiently
- **Asynchronous Processing**: Job-based processing with real-time progress tracking via Server-Sent Events (SSE)
- **Job Management**: Cancel running jobs and track processing status through the API
- **API Documentation**: Automatic OpenAPI/Swagger documentation for easy integration
- **Docker Deployment**: Provided as a Docker image for simple setup and deployment
- **Custom Lookup Tables**: Extended Dutch name databases for improved detection accuracy

## Getting Started

Follow the extended instructions on the [wiki](https://github.com/Carmenda-nl/Carmenda_pseudonymize/wiki), or refer to
one of the sections in this README. For building an executable to use with the frontend, please consult the Wiki.

For a detailed list of changes and version history, see the [CHANGELOG](CHANGELOG.md).

## How It Works

The Privacytool uses the Deduce algorithm to detect and replace sensitive information in textual data with pseudonyms.  
Enhanced with custom Dutch lookup tables for names, locations, and medical institutions
the tool provides improved accuracy for Dutch healthcare data.  
This method ensures that the data remains useful for analytical purposes while safeguarding individual privacy.

## License

This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later.  
© 2026 Carmenda. All rights reserved.

## Contact

For questions or support, please contact us at [support@carmenda.nl](mailto:support@carmenda.nl).

---

## Deployment (Docker)

### Build the Docker Image

Build the backend image from the Dockerfile:

```bash
# Run this command from the project root directory
docker build -f deployment/Dockerfile -t privacy-backend:latest .
```

### Run the Privacy Tool

Use the following command to run the privacy tool:

```bash
docker run -it --rm -p 8000:8000 -e DEBUG=True privacy-backend:latest
```

The API will be available at `http://localhost:8000/`

> **Note:** Set `DEBUG=False` for production environments.

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

## Development

### Step 1: Preparations

Ensure Python is installed (minimum version 3.10) and Git.
Clone this repository to your local machine:

```bash
git clone --recursive https://github.com/Carmenda-nl/carmenda_pseudonymize_core.git
```

Open a terminal and navigate to the `app` folder of this project:

```bash
cd carmenda_pseudonymize_core/app
```

Create a virtual environment, as all dependencies need to be loaded into it:

```bash
python virtualenv .venv
```

Activate the virtual environment:

**Windows (Command Prompt):**

```bash
.venv\Scripts\activate
```

**Windows (PowerShell):**

```bash
.venv\Scripts\Activate.ps1
```

**Linux/macOS:**

```bash
source .venv/bin/activate
```

### Step 2: Install dependencies

Install the project dependencies:

```bash
pip install -r requirements.txt
```

### Step 3: Configuration

Create an `.env` file in the `app` folder with the following configuration:

```env
DJANGO_RUNSERVER_HIDE_WARNING=true

DEBUG=False
LOG_LEVEL=INFO

SECRET_KEY=your-secret-key-here
CSRF_TRUSTED_ORIGINS=http://127.0.0.1

JOB_LOG_ONLY=True
```

> **Note:** Replace `your-secret-key-here` with a secure random string. For production environments, ensure `DEBUG=False` and use appropriate CSRF trusted origins.

### Step 4: Run the Server

You can now test-run the server to verify everything functions properly:

```bash
python manage.py runserver
```

The API will be available at `http://127.0.0.1:8000/`
