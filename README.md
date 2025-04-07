
# Carmenda Pseudonymize

Carmenda Pseudonymize is a solution designed to pseudonymize textual data for care organizations.
This tool leverages the Deduce algorithm to effectively mask sensitive information, ensuring compliance with data privacy regulations.
Built with PySpark for enhanced performance, it is ideal for handling large datasets efficiently.

## Features

- **Pseudonymization of Textual Data**: Effectively masks sensitive information using the Deduce algorithm.
- **High Performance**: Utilizes PySpark to handle large datasets with speed and efficiency.
- **Easy Deployment**: Available as a Docker image for easy setup and deployment.

## Getting Started

Follow the instruction on the [wiki](https://github.com/Carmenda-nl/Carmenda_pseudonymize/wiki)

## How It Works

Carmenda Pseudonymize uses the Deduce algorithm to replace sensitive information in text data with pseudonyms. This method ensures that the data remains useful for analytical purposes while protecting individual privacy

## License

This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later
Copyright (c) 2025 Carmenda. All rights reserved.

## Contact

If you have any questions or need further assistance, please reach out to us at [Carmenda](support@carmenda.nl)

---

## Deployment

Use following commands in terminal to build and run container

Build image from this Dockerfile (replace latest with other tag value if you want)
`docker build -t img_name:latest .`

### Run the image

The first `-v` makes/connects a Docker Volume to the `/data/input` folder within the container
> *A Docker Volume is disk space managed by docker to be shared by host and container. This avoids unexpected changes to other host disk locations.*

The second `-v` connects your local folder with output data to the `/data/input` folder in the container.
> *Instead of a folder an individual file should also work.*

Note: that ${pwd} notation is powershell specific, try out what works in your terminal or use full file path.

- `pyspark_deducer_app` is the name of the image, `:latest` indicates a tag.
- `input.csv` is an argument for the code execution, that will be added to the CMD command above.
- Replace arguments with `/bin/bash` and add -it to run command to avoid entering the program and open a terminal inside the container.

### Normal, simple situation

```docker run -v deducerVol:/data -v ${pwd}/data/input:/data/input  img_name input.csv```

### Interactive terminal, binding down to a specific file (second -v), calling a specific image version using :latest tag

```docker run -it -v deducerVol:/data -v ${pwd}/data/input.csv:/data/input/input.csv --entrypoint /bin/bash img_name:latest```

### For help with the python script

```docker run --entrypoint python img_name pyspark_deducer_baked.py --help```

### For mounting the app directory and running a custom script

```docker run -v deducerVol:/data -v ${pwd}/data/input:/data/input -v ${pwd}/app:/app --entrypoint python img_name your_script.py```
