# Max NHL Scraper

The `max_nhl_scraper` package is a Python tool for scraping NHL (National Hockey League) data. It allows users to easily extract detailed game data, player statistics, and other relevant information from the NHL's official sources.

which can be used in training machine learning models and so.

## Installation

You can install max_nhl_scraper directly from PyPI:

```bash
pip install max_nhl_scraper
```
# Using Max_NHL_Scraper in Python

Here's a simple example of how to use max_nhl_scraper::

1. Import the `MaxNHLScraper` class from the `max_nhl_scraper` module.
2. Create an instance of the scraper using `MaxNHLScraper()`.
3. Use the instance to scrape data for a specific game or player.

```python
from max_nhl_scraper import MaxNHLScraper

# Create an instance of the scraper
scraper = MaxNHLScraper()

# Example: Scrape data for a specific game or player
data = scraper.scrape_game(game_id=2023020005) # Replace <2023020005> with the actual game ID you want to scrape data for.
```

## Requirements

max_nhl_scraper requires the following Python libraries:

- pandas
- numpy
- requests
- BeautifulSoup

These dependencies should be automatically installed when you install the package using pip.

## License
This project is licensed under the MIT License - view the LICENSE file for more details.

## Contributing
Contributions to max_nhl_scraper are welcome! Please feel free to submit pull requests or open issues to discuss potential improvements or report bugs in this project.

## Contact
If you have any quries or suggestions on this project, feel free to contact the author at maxtixador@gmail.com.

## Acknowledgments
Special thanks to all contributors (just me for now)who have gave their valuable effort and users of max_nhl_scraper.
