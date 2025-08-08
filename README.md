# Carmenda Pseudonymize

![Linter](https://img.shields.io/badge/linter-flake8-4B8B3B)

**Carmenda Pseudonymize** is a solution designed to pseudonymize textual data for care organizations.  
This tool leverages the **[Deduce](https://github.com/vmenger/deduce)** tool (Menger et al. 2017) [1] algorithm to effectively mask sensitive information, ensuring compliance with data privacy regulations.  
Built with **Polars** for enhanced performance, it is ideal for efficiently handling large datasets.

## Features

- **Pseudonymization of Textual Data**: Masks sensitive information using the Deduce algorithm.
- **High Performance**: Utilizes Polars to process large datasets quickly and efficiently.
- **Easy Deployment**: Provided as a Docker image for simple setup and deployment.

## Getting Started

Follow the instructions on the [wiki](https://github.com/Carmenda-nl/Carmenda_pseudonymize/wiki).

## How It Works

Carmenda Pseudonymize uses the Deduce algorithm to replace sensitive information in textual data with pseudonyms.  
This method ensures that the data remains useful for analytical purposes while safeguarding individual privacy.

## License

This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later.  
© 2025 Carmenda. All rights reserved.

## Contact

For questions or support, please contact us at [support@carmenda.nl](mailto:support@carmenda.nl).

---

## Deployment

Use the following commands in the terminal to build and run the container.

### Build the Docker Image

Build the image from the Dockerfile (replace `latest` with a different tag if needed):

```bash
docker build -t carmenda/core:latest .
```

### Run the Image

The first `-v` makes/connects a Docker Volume to the `/data/input` folder within the container  
> *A Docker Volume is disk space managed by Docker to be shared by the host and container. This avoids unexpected changes to other host disk locations.*

The second `-v` connects your local folder with output data to the `/data/input` folder in the container.  
> *Instead of a folder, an individual file should also work.*

**Note:** The `${pwd}` notation is PowerShell-specific. Try what works in your terminal or use the full file path.

- `img_name:latest` is the name of the image, with `:latest` indicating a tag.
- `input.csv` is an argument for the code execution, which will be added to the CMD command above.
- Replace arguments with `/bin/bash` and add `-it` to the run command to avoid entering the program and open a terminal inside the container.

### Normal, Simple Situation

```bash
docker run -v deducerVol:/data -v ${pwd}/data/input:/data/input img_name input.csv
```

> example:

```bash
docker run -v "/mnt/c/Users/djang/Documents/data/input:/data/input" -v "/mnt/c/Users/djang/Documents/data/output:/data/output" core:latest --input_fofi /data/input/dummy_input.csv --input_cols "patientName=Clientnaam, report=Rapportage"
```

### Interactive terminal, binding down to a specific file (second -v), calling a specific image version using :latest tag

```bash
docker run -it -v deducerVol:/data -v ${pwd}/data/input.csv:/data/input/input.csv --entrypoint /bin/bash img_name:latest
```

### For help with the python script

```bash
docker run --entrypoint python img_name polars_deduce_baked.py --help
```

### For mounting the app directory and running a custom script

```bash
docker run -v deducerVol:/data -v ${pwd}/data/input:/data/input -v ${pwd}/app:/app --entrypoint python img_name your_script.py
```

[1] Menger, V.J., Scheepers, F., van Wijk, L.M., Spruit, M. (2017). DEDUCE: A pattern matching method for automatic de-identification of Dutch medical text, Telematics and Informatics, 2017, ISSN 0736-5853
