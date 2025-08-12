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

## Docker Deployment

### Build the Docker Image

Build the image from the Dockerfile:

```bash
docker build -t privacy-core:latest .
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
  -v /path/to/your/input:/source/data/input \
  -v /path/to/your/output:/source/data/output \
  carmenda-pseudonymizer \
  --input_fofi /source/data/input/your_file.csv \
  --output_extension .csv
```

**Example:**

```bash
docker run --rm \
  -v /Users/h.j.m.tummers/docker_test/data/input:/source/data/input \
  -v /Users/h.j.m.tummers/docker_test/data/output:/source/data/output \
  carmenda-pseudonymizer \
  --input_fofi /source/data/input/test_SHL.csv \
  --output_extension .csv
```

### Additional Options

For custom column mappings:

```bash
docker run --rm \
  -v /path/to/your/input:/source/data/input \
  -v /path/to/your/output:/source/data/output \
  carmenda-pseudonymizer \
  --input_fofi /source/data/input/your_file.csv \
  --input_cols "patientName=Cliëntnaam, report=rapport" \
  --output_cols "patientID=patientID, processed_report=processed_report" \
  --output_extension .csv
```

### Important Notes

- **Input Path**: Always use `/source/data/input/` as the base path for input files in the container
- **Output Path**: Always use `/source/data/output/` as the base path for output files in the container  
- **Volume Mounting**: The `-v` flags map your local directories to the container directories
- **File Extensions**: Supported formats are `.csv` and `.parquet`
- **Remove Container**: The `--rm` flag automatically removes the container after execution

### Troubleshooting

If you don't see output files:

1. Check that the volume mounts are correct
2. Verify that the input file path starts with `/source/data/input/`
3. Ensure the output directory has write permissions
4. Check the container logs for any error messages

[1] Menger, V.J., Scheepers, F., van Wijk, L.M., Spruit, M. (2017). DEDUCE: A pattern matching method for automatic de-identification of Dutch medical text, Telematics and Informatics, 2017, ISSN 0736-5853
