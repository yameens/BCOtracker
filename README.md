# Forum Mobility ESG Port Analytics

A beautiful web application for tracking sustainable shipping routes and port utilization for ESG-compliant companies. Built with Python, Flask, and modern web technologies.

## Features

- **Company Selection**: Dropdown menu with 100+ ESG-compliant companies
- **Port Analytics**: Top export/import ports with shipment volumes
- **Trade Lanes**: Detailed shipping routes between ports
- **Real-time Data**: Live scraping from ImportYeti
- **Caching**: Smart caching for improved performance
- **Responsive Design**: Works on desktop and mobile devices
- **Beautiful UI**: Modern, aesthetic design with smooth animations

## Screenshots

The application features:
- Gradient background with Forum Mobility branding
- Clean, card-based layout
- Interactive company selector
- Real-time statistics dashboard
- Detailed port and lane information
- Loading states and error handling

## Prerequisites

Before running the application, make sure you have:

1. **Python 3.8+** installed
2. **Chrome browser** installed
3. **ChromeDriver** installed (matches your Chrome version)

## Installation

1. **Clone or download** the project files to your local machine

2. **Install Python dependencies**:
   ```bash
   pip install selenium beautifulsoup4 flask flask-cors requests brotli
   ```

3. **Install ChromeDriver** (macOS):
   ```bash
   brew install chromedriver
   ```

## Setup

1. **Start Chrome with remote debugging**:
   ```bash
   open -na "Google Chrome" --args \
     --remote-debugging-port=9222 \
     --user-data-dir="$HOME/ChromeScrapeProfile"
   ```

2. **Log into ImportYeti** in the Chrome window that opens

3. **Set environment variable** (optional):
   ```bash
   export CHROME_DEBUGGER=127.0.0.1:9222
   ```

## Running the Application

1. **Start the Flask server**:
   ```bash
   python app.py
   ```

2. **Open your browser** and navigate to:
   ```
   http://localhost:5000
   ```

3. **Select a company** from the dropdown to view their port analytics

## How It Works

### Backend (Python/Flask)
- **Web Scraping**: Uses Selenium with Chrome DevTools Protocol to scrape ImportYeti
- **API Endpoints**: RESTful API for company list and port data
- **Caching**: Smart caching to avoid repeated scraping
- **Data Processing**: Extracts and formats port/lane information

### Frontend (HTML/CSS/JavaScript)
- **Modern UI**: Beautiful gradient design with smooth animations
- **Real-time Updates**: Dynamic loading of company data
- **Responsive**: Works on all device sizes
- **Error Handling**: Graceful error states and loading indicators

## API Endpoints

- `GET /` - Main application page
- `GET /api/companies` - List of available companies
- `GET /api/company/{name}` - Port data for specific company
- `GET /api/health` - Health check endpoint
- `GET /api/cache/clear` - Clear the data cache

## Data Structure

The application provides three types of data for each company:

### Export Ports
```json
{
  "port": "Yantian",
  "shipments": 1247
}
```

### Import Ports
```json
{
  "port": "Los Angeles", 
  "shipments": 1567
}
```

### Trade Lanes
```json
{
  "exit_port": "Yantian",
  "entry_port": "Los Angeles",
  "shipments": 892
}
```

## Troubleshooting

### Common Issues

1. **Chrome not starting with debugging**:
   - Make sure Chrome is completely closed before running the command
   - Check that the port 9222 is not already in use

2. **"ChromeDriver not found" error**:
   - Install ChromeDriver: `brew install chromedriver`
   - Make sure ChromeDriver version matches your Chrome version

3. **"No data available" for companies**:
   - Ensure you're logged into ImportYeti in the Chrome window
   - Check that the company exists on ImportYeti
   - Try refreshing the page and selecting again

4. **Flask server won't start**:
   - Check that port 5000 is not in use
   - Make sure all dependencies are installed
   - Try running with `python3 app.py`

### Debug Mode

For debugging, you can run the scraper directly:

```bash
# Test with a single company
python debug_single_company.py "Kimberly-Clark"

# Analyze network requests
python analyze_network.py

# Run the full scraper
python web_ports_extractor.py
```

## File Structure

```
validBCO/
├── app.py                 # Flask backend server
├── BCOdata_cdp.py        # Main scraping logic
├── consumerBCO.txt       # List of companies
├── templates/
│   └── index.html        # Frontend application
├── captures/             # Debug JSON captures
├── debug_single_company.py
├── analyze_network.py
└── README.md
```

## Customization

### Adding Companies
Edit `consumerBCO.txt` to add or remove companies from the list.

### Styling
Modify the CSS in `templates/index.html` to customize the appearance.

### API Configuration
Adjust caching, timeouts, and other settings in `app.py`.

## Performance

- **Caching**: Data is cached for 1 hour to reduce scraping
- **Lazy Loading**: Companies are loaded on-demand
- **Error Recovery**: Graceful handling of network issues
- **Memory Efficient**: Minimal memory footprint

## Security

- **CORS Enabled**: Cross-origin requests allowed for development
- **Input Validation**: Company names are validated before processing
- **Error Handling**: Sensitive information is not exposed in errors

## Development

To contribute to the project:

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

## License

© 2024 Forum Mobility. ESG-focused port analytics for sustainable supply chains.

## Support

For issues or questions:
1. Check the troubleshooting section above
2. Review the debug scripts for detailed error information
3. Ensure all prerequisites are properly installed

---

**Forum Mobility** - Driving sustainable supply chain analytics for a greener future.
