OMIE downloader notebook

This folder contains `omie_downloader.ipynb`, a Jupyter notebook that:

- Scrapes the OMIE file-access listing for the "Precios horarios del mercado diario en España" directory.
- Lists the downloadable files (CSV, ZIP, XLSX, etc.).
- Downloads selected files into `data/omie` with retries and progress reporting.

Requirements

- Python 3.9+ (or matching your project env)
- Install dependencies:
  - requests
  - beautifulsoup4
  - pandas
  - tqdm

Usage

1. Open the notebook `omie_downloader.ipynb` in Jupyter or VS Code.
2. Run cells sequentially. The notebook will fetch file links and show a DataFrame of candidates.
3. Adjust `N` in the example download cell to download more files or use `download_file()` directly.

Notes

- The notebook uses HTML parsing; it may need tweaks if OMIE changes the page structure.
- Respect OMIE terms of service and rate limits when downloading. Add delays or caching if running repeatedly.
