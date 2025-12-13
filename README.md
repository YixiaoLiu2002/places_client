# places_client

A Python package for accessing county-level data from the **CDC PLACES** project's API. 

It focuses exclusively on measures categorized as **Health Outcomes** and **Health Risk Behaviors**, allowing users to access and filter these key public health indicators and explore the relationship between these measures.

## Features
- Retrieve county-level places data for a specified release (2020-2025).
- Display key info of supported measures (id, short name, full name, and category).
- Filter data by measures, categories, counties, or states.
- Create wide pivot tables for measures at county or state levels.
- Calculate correlations between measures.
- Get the descriptive statistics of meausures.

## Installation

```bash
pip install places_client
```

## Usage
```python
from places_client.places_client import PlacesClient
import os
from dotenv import load_dotenv

load_dotenv()
client = PlacesClient(os.getenv("CDC_API_TOKEN"))

df = client.get_county_data('2024')
measures = client.get_measure_list()
```

For detailed usage, view this vignette: [vignette.ipynb](https://github.com/YixiaoLiu2002/places_client/blob/main/vignette.ipynb)

## Contributing

Interested in contributing? Check out the contributing guidelines. Please note that this project is released with a Code of Conduct. By contributing to this project, you agree to abide by its terms.

## License

`places_client` was created by Yixiao Liu. It is licensed under the terms of the MIT license.

## Credits

`places_client` was created with [`cookiecutter`](https://cookiecutter.readthedocs.io/en/latest/) and the `py-pkgs-cookiecutter` [template](https://github.com/py-pkgs/py-pkgs-cookiecutter).
