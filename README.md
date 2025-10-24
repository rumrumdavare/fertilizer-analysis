# 🌍 Global Fertilizer Consumption Analysis

An interactive web application that analyzes global fertilizer consumption patterns using World Bank data. Built with Python, SQL, DuckDB, and Streamlit.

![Streamlit](https://img.shields.io/badge/Streamlit-FF4B4B?style=for-the-badge&logo=Streamlit&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)

## 🚀 Live App

**[View the live application here](https://rumrumdavare-fertilizer-analysis-app.streamlit.app/)**

*Note: First load may take 2-3 minutes as it downloads fresh data from World Bank API*

## 📊 Features

- **🌐 Interactive World Map** - Choropleth map with time slider showing global consumption patterns
- **📈 Country Trends** - Compare fertilizer usage across multiple countries over time
- **🔥 Change Analysis** - Identify countries with largest consumption increases/decreases
- **📊 Overview Dashboard** - Key metrics and top consumer rankings
- **🔄 Real-time Data** - Always fetches latest data from World Bank API

## 🛠️ Installation (Local Development)

1. **Clone the repository**
```bash
git clone https://github.com/rumrumdavare/fertilizer-analysis.git
cd fertilizer-analysis
```

2. **Install dependencies**
```bash
pip install -r requirements.txt
```

3. **Run the application**
```bash
streamlit run app.py
```

## 📁 Project Structure

fertilizer-analysis/
├── fertilizer_app.py          # Main Streamlit application
├── fertilizer_sql_analysis.py # ETL pipeline & analysis functions
├── requirements.txt           # Python dependencies
└── README.md                  # This file

## 🗃️ Data Sources

- **World Bank API** - Fertilizer consumption indicators (AG.CON.FERT.ZS)
- **Country metadata** - Regional classifications and ISO codes

## 🎯 Usage

1. Select analysis type from the sidebar
2. Filter by year range and region
3. Interact with visualizations - hover, click, zoom
4. Download insights for further analysis

## 📈 Analysis Types

1. **Overview Dashboard**: Top consumers and key metrics
2. **World Map**: Global consumption patterns over time
3. **Country Trends**: Compare multiple countries' fertilizer usage
4. **Change Analysis**: Identify emerging trends and reductions

## 🔧 Technical Details
 - **Backend**: Python, DuckDB, SQL, Pandas
 - **Frontend**: Streamlit, Plotly, Matplotlib
 - **Data**: World Bank API, Real-time ETL
 - **Deployment**: Streamlit Cloud

## 🤝 Contributing
Feel free to fork this project and submit pull requests for any improvements!

## 📄 License
This project is licensed under the [Creative Commons Zero v1.0 Universal](LICENSE) license.

Built with ❤️ using Streamlit, SQL, and World Bank data