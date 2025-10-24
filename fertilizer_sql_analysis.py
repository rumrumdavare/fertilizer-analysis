import requests
import pandas as pd
import duckdb
import matplotlib.pyplot as plt
from plotly.graph_objects import Figure
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

WB_URL = "https://api.worldbank.org/v2"
WB_INDICATOR = "AG.CON.FERT.ZS"  # fertilizer consumption (kg/ha of arable land)
DB_PATH = "fertilizer.duckdb"


def load_api_to_duckdb(db_path: str = DB_PATH):
    """Load World Bank fertilizer and country data into DuckDB raw tables."""
    
    # Fetch fertilizer indicator data (all countries, all years)
    fert = []
    page = 1
    while True:
        r = requests.get(
            f"{WB_URL}/country/ALL/indicator/{WB_INDICATOR}",
            params={"format": "json", "per_page": 50, "page": page},
            timeout=60,
        )
        r.raise_for_status()
        payload = r.json()
        if not isinstance(payload, list) or len(payload) < 2 or not payload[1]:
            break
        fert.extend(payload[1])
        if page >= payload[0]["pages"]:
            break
        page += 1
    df_f = pd.json_normalize(fert)

    # Fetch country master data
    r = requests.get(
        f"{WB_URL}/country",
        params={"format": "json", "per_page": 20000},
        timeout=60
    )
    r.raise_for_status()
    payload = r.json()
    rows = payload[1] if isinstance(payload, list) and len(payload) > 1 else []
    
    # Create tidy country dataframe
    df_c = pd.DataFrame([{
        "iso3": rec["id"],
        "iso2": rec["iso2Code"],
        "name": rec["name"],
        "region": (rec.get("region") or {}).get("value")
    } for rec in rows])

    # Save to DuckDB
    con = duckdb.connect(db_path)
    con.execute("CREATE SCHEMA IF NOT EXISTS wb;")
    con.register("df_f", df_f)
    con.register("df_c", df_c)
    con.execute("CREATE OR REPLACE TABLE wb.raw_fert AS SELECT * FROM df_f;")
    con.execute("CREATE OR REPLACE TABLE wb.raw_country AS SELECT * FROM df_c;")
    con.close()
    
    print(f"✓ Loaded raw data to {db_path}")


def clean_with_sql(db_path: str = DB_PATH):
    """Clean and join fertilizer and country data into a final table."""
    
    con = duckdb.connect(db_path)
    con.execute("""
    CREATE OR REPLACE TABLE wb.fertilizer_clean AS
    WITH fert AS (
        SELECT
            countryiso3code AS iso3,
            "country.value" AS country_name_api,
            CAST(date AS INTEGER) AS year,
            CAST(value AS DOUBLE) AS kg_per_ha
        FROM wb.raw_fert
        WHERE value IS NOT NULL
    ),
    countries AS (
        SELECT iso3, iso2, name, region
        FROM wb.raw_country
        WHERE region IS NOT NULL AND region <> 'Aggregates'
    )
    SELECT
        c.iso2,
        c.iso3,
        COALESCE(c.name, f.country_name_api) AS country_name,
        c.region,
        f.year,
        CAST(ROUND(f.kg_per_ha) AS INTEGER) AS kg_per_ha
    FROM fert f
    JOIN countries c USING (iso3);
    """)
    con.close()
    
    print(f"✓ Created clean table in {db_path}")


def verify_data(db_path: str = DB_PATH):
    """Verify the cleaned data."""
    
    con = duckdb.connect(db_path)
    
    # Row count
    count = con.execute("SELECT COUNT(*) FROM wb.fertilizer_clean").fetchone()[0]
    print(f"\nTotal rows: {count:,}")
    
    # Year range
    min_year, max_year = con.execute(
        "SELECT MIN(year), MAX(year) FROM wb.fertilizer_clean"
    ).fetchone()
    print(f"Year range: {min_year} - {max_year}")
    
    # Sample data
    print("\nSample data:")
    print(con.execute("SELECT * FROM wb.fertilizer_clean LIMIT 5").df())
    
    con.close()

load_api_to_duckdb()
clean_with_sql()
verify_data()

def visualize_top_consumers_2020(db_path: str = DB_PATH, top_n: int = 20):
    """Show countries with highest fertilizer consumption in 2020."""
    
    con = duckdb.connect(db_path)
    
    df = con.execute(f"""
    SELECT 
        country_name,
        region,
        kg_per_ha
    FROM wb.fertilizer_clean
    WHERE year = 2020
    ORDER BY kg_per_ha DESC
    LIMIT {top_n}
    """).df()
    
    con.close()
    
    # Display the table
    print(f"\n{'='*70}")
    print(f"TOP {top_n} COUNTRIES BY FERTILIZER CONSUMPTION (2020)")
    print(f"{'='*70}\n")
    
    # Format the output
    print(f"{'Rank':<6} {'Country':<30} {'Region':<25} {'kg/ha':>8}")
    print(f"{'-'*6} {'-'*30} {'-'*25} {'-'*8}")
    
    for idx, row in df.iterrows():
        rank = idx + 1
        country = row['country_name'][:28]  # Truncate long names
        region = row['region'][:23] if row['region'] else 'N/A'
        kg = row['kg_per_ha']
        print(f"{rank:<6} {country:<30} {region:<25} {kg:>8,}")
    
    print(f"\n{'='*70}\n")
    
    return df


# Show visualization
# df_top = visualize_top_consumers_2020(top_n=20)
    
    # Optional: Access the dataframe for further analysis
    # print(df_top.describe())

def visualize_trend_line_chart(db_path: str = DB_PATH, countries: list = None, year_start: int = 1990, year_end: int = 2023):
    """Create a line chart showing fertilizer consumption trends over time.
    
    Args:
        db_path: Path to DuckDB database
        countries: List of country names to plot. If None, shows top 10 consumers in latest year
        year_start: Start year for the chart
        year_end: End year for the chart
    """

    con = duckdb.connect(db_path)
    
    # If no countries specified, get top 10 from most recent year with data
    if countries is None:
        top_countries = con.execute("""
        SELECT country_name
        FROM wb.fertilizer_clean
        WHERE year = (SELECT MAX(year) FROM wb.fertilizer_clean WHERE kg_per_ha IS NOT NULL)
        ORDER BY kg_per_ha DESC
        LIMIT 10
        """).df()
        countries = top_countries['country_name'].tolist()
    
    # Fetch data for selected countries
    country_list = "','".join(countries)
    df = con.execute(f"""
    SELECT 
        country_name,
        year,
        kg_per_ha
    FROM wb.fertilizer_clean
    WHERE country_name IN ('{country_list}')
        AND year BETWEEN {year_start} AND {year_end}
    ORDER BY country_name, year
    """).df()
    
    con.close()
    
    # Create the plot
    plt.figure(figsize=(14, 8))
    
    # Plot each country
    for country in countries:
        country_data = df[df['country_name'] == country]
        plt.plot(country_data['year'], country_data['kg_per_ha'], 
                marker='o', markersize=3, linewidth=2, label=country)
    
    # Styling
    plt.title(f'Fertilizer Consumption Trends ({year_start}-{year_end})', 
             fontsize=16, fontweight='bold', pad=20)
    plt.xlabel('Year', fontsize=12, fontweight='bold')
    plt.ylabel('Fertilizer Consumption (kg/ha)', fontsize=12, fontweight='bold')
    plt.legend(loc='best', fontsize=10, framealpha=0.9)
    plt.grid(True, alpha=0.3, linestyle='--')
    plt.tight_layout()
    
    # Save and show
    plt.savefig('fertilizer_trends.png', dpi=300, bbox_inches='tight')
    print(f"\n✓ Chart saved as 'fertilizer_trends.png'")
    plt.show()
    
    return df

# visualize_trend_line_chart()

def peak_consumption_advanced_interactive(db_path: str = DB_PATH, top_n: int = 20):
    """More advanced interactive version with custom selection behavior."""
    
    con = duckdb.connect(db_path)
    df = con.execute("""
    WITH ranked AS (
        SELECT country_name, region, year as peak_year, kg_per_ha as peak_consumption,
               ROW_NUMBER() OVER (PARTITION BY country_name ORDER BY kg_per_ha DESC) as rank
        FROM wb.fertilizer_clean WHERE year >= 1970
    )
    SELECT *, 
           CASE WHEN peak_consumption > 500 THEN 'Very High'
                WHEN peak_consumption > 200 THEN 'High'
                WHEN peak_consumption > 100 THEN 'Medium'
                ELSE 'Low' END as consumption_level
    FROM ranked WHERE rank = 1
    ORDER BY peak_consumption DESC LIMIT ?
    """, [top_n]).df()
    con.close()
    
    fig = px.bar(
        df, x='peak_consumption', y='country_name',
        color='consumption_level',
        color_discrete_map={
            'Very High': "#000dff",
            'High': "#00a2ff", 
            'Medium': "#00ff4c",
            'Low': "#e1ff00"
        },
        hover_data=['region', 'peak_year'],
        title=f'Interactive Peak Fertilizer Consumption Analysis'
    )
    
    fig.show()
    return df, fig

# peak_consumption_advanced_interactive()

# def regional_heatmap_advanced(db_path: str = DB_PATH):
#     """Advanced interactive heatmap with annotations and trends."""
    
#     con = duckdb.connect(db_path)
#     df = con.execute("""
#     WITH region_data AS (
#         SELECT 
#             region,
#             FLOOR(year/10)*10 as decade,
#             AVG(kg_per_ha) as avg_consumption,
#             COUNT(*) as country_count,
#             MIN(kg_per_ha) as min_consumption,
#             MAX(kg_per_ha) as max_consumption
#         FROM wb.fertilizer_clean
#         WHERE year >= 1990
#         GROUP BY region, decade
#         HAVING COUNT(*) > 5
#     )
#     SELECT *,
#            (SELECT AVG(kg_per_ha) 
#             FROM wb.fertilizer_clean f2 
#             WHERE f2.region = region_data.region 
#             AND f2.year BETWEEN region_data.decade AND region_data.decade+9) as exact_avg
#     FROM region_data
#     ORDER BY decade, region
#     """).df()
#     con.close()
    
#     # Pivot for heatmap
#     pivot_df = df.pivot(index='region', columns='decade', values='avg_consumption')
    
#     fig = px.imshow(
#         pivot_df,
#         color_continuous_scale="RdYlBu_r",  # Reversed for better interpretation
#         aspect="auto",
#         title="<b>Fertilizer Consumption Heatmap</b><br><sub>Regional trends from 1990s to 2020s</sub>",
#         labels=dict(x="Decade", y="Region", color="kg/ha")
#     )
    
#     # Enhanced hover with more details
#     fig.update_traces(
#         hovertemplate=(
#             "<b>%{y}</b> in %{x}s<br>"
#             "Average: <b>%{z:.0f} kg/ha</b><br>"
#             "Countries with data: %{customdata[0]}<br>"
#             "Range: %{customdata[1]:.0f}-%{customdata[2]:.0f} kg/ha<br>"
#             "<extra></extra>"
#         ),
#         customdata=df[['country_count', 'min_consumption', 'max_consumption']].values.reshape(
#             pivot_df.shape[0], pivot_df.shape[1], 3
#         )
#     )
    
#     # Professional styling
#     fig.update_layout(
#         xaxis_title="Decade",
#         yaxis_title="Region",
#         height=700,
#         font=dict(size=12),
#         coloraxis_colorbar=dict(
#             title="kg/ha",
#             thickness=20,
#             len=0.75
#         )
#     )
    
#     # Add annotations for significant changes
#     fig.add_annotation(
#         x=0.02, y=0.98,
#         xref="paper", yref="paper",
#         text="💡 Hover for detailed stats<br>🔍 Click+drag to zoom",
#         showarrow=False,
#         bgcolor="white",
#         bordercolor="black",
#         borderwidth=1
#     )
    
#     fig.show()
#     return df, fig

# # Execute advanced version
# advanced_df, advanced_fig = regional_heatmap_advanced()

def consumption_change_analysis(db_path: str = DB_PATH, year_start: int = 2010, year_end: int = 2020):
    """Analyze countries with largest consumption increases/decreases in the last decade."""
    
    con = duckdb.connect(db_path)
    
    df = con.execute("""
    WITH latest_data AS (
        SELECT 
            country_name,
            region,
            year,
            kg_per_ha,
            LAG(kg_per_ha) OVER (PARTITION BY country_name ORDER BY year) as prev_kg_per_ha
        FROM wb.fertilizer_clean
        WHERE year IN (?, ?)
    ),
    changes AS (
        SELECT 
            country_name,
            region,
            MAX(CASE WHEN year = ? THEN kg_per_ha END) as start_consumption,
            MAX(CASE WHEN year = ? THEN kg_per_ha END) as end_consumption,
            (MAX(CASE WHEN year = ? THEN kg_per_ha END) - 
             MAX(CASE WHEN year = ? THEN kg_per_ha END)) as absolute_change,
            ROUND(((MAX(CASE WHEN year = ? THEN kg_per_ha END) - 
                   MAX(CASE WHEN year = ? THEN kg_per_ha END)) / 
                   NULLIF(MAX(CASE WHEN year = ? THEN kg_per_ha END), 0)) * 100, 1) as percent_change
        FROM latest_data
        GROUP BY country_name, region
        HAVING COUNT(DISTINCT year) = 2  -- Only countries with data for both years
    )
    SELECT *
    FROM changes
    WHERE start_consumption IS NOT NULL AND end_consumption IS NOT NULL
    ORDER BY absolute_change DESC
    """, [year_start, year_end, year_start, year_end, year_end, year_start, year_end, year_start, year_start]).df()
    
    con.close()
    
    # Display results
    print(f"\n{'='*80}")
    print(f"LARGEST CONSUMPTION CHANGES ({year_start}-{year_end})")
    print(f"{'='*80}")
    
    print(f"\n📈 TOP 10 INCREASES:")
    print(f"{'Rank':<6} {'Country':<25} {'Region':<20} {'Start':>8} {'End':>8} {'Change':>8} {'% Change':>10}")
    print(f"{'-'*6} {'-'*25} {'-'*20} {'-'*8} {'-'*8} {'-'*8} {'-'*10}")
    
    increases = df[df['absolute_change'] > 0].head(10)
    for idx, row in increases.iterrows():
        rank = idx + 1
        print(f"{rank:<6} {row['country_name'][:24]:<25} {row['region'][:19]:<20} "
              f"{row['start_consumption']:>8,} {row['end_consumption']:>8,} "
              f"{row['absolute_change']:>+8,} {row['percent_change']:>+9}%")
    
    print(f"\n📉 TOP 10 DECREASES:")
    print(f"{'Rank':<6} {'Country':<25} {'Region':<20} {'Start':>8} {'End':>8} {'Change':>8} {'% Change':>10}")
    print(f"{'-'*6} {'-'*25} {'-'*20} {'-'*8} {'-'*8} {'-'*8} {'-'*10}")
    
    decreases = df[df['absolute_change'] < 0].head(10)
    for idx, row in decreases.iterrows():
        rank = idx + 1
        print(f"{rank:<6} {row['country_name'][:24]:<25} {row['region'][:19]:<20} "
              f"{row['start_consumption']:>8,} {row['end_consumption']:>8,} "
              f"{row['absolute_change']:>+8,} {row['percent_change']:>+9}%")
    
    # Create visualization
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
    
    # Top increases
    top_inc = increases.head(8)
    bars1 = ax1.barh(top_inc['country_name'], top_inc['absolute_change'], color='green', alpha=0.7)
    ax1.set_title(f'Largest Consumption Increases\n({year_start}-{year_end})', fontweight='bold')
    ax1.set_xlabel('Increase (kg/ha)')
    ax1.bar_label(bars1, fmt='+%.0f', padding=3)
    
    # Top decreases
    top_dec = decreases.head(8)
    bars2 = ax2.barh(top_dec['country_name'], top_dec['absolute_change'], color='red', alpha=0.7)
    ax2.set_title(f'Largest Consumption Decreases\n({year_start}-{year_end})', fontweight='bold')
    ax2.set_xlabel('Decrease (kg/ha)')
    ax2.bar_label(bars2, fmt='%.0f', padding=3)
    
    plt.tight_layout()
    plt.savefig('consumption_changes.png', dpi=300, bbox_inches='tight')
    print(f"\n✓ Chart saved as 'consumption_changes.png'")
    plt.show()
    
    return df

# Execute the change analysis
# change_df = consumption_change_analysis(year_start=2010, year_end=2020)

def world_map_with_timeslider(db_path: str = DB_PATH):
    """Interactive choropleth map with time slider to show evolution."""
    
    con = duckdb.connect(db_path)
    df = con.execute("""
    SELECT 
        iso3,
        country_name, 
        region,
        year,
        kg_per_ha
    FROM wb.fertilizer_clean
    WHERE year >= 1990 AND kg_per_ha IS NOT NULL
    ORDER BY year, kg_per_ha DESC
    """).df()
    con.close()
    
    fig = px.choropleth(
        df,
        locations="iso3",
        color="kg_per_ha", 
        hover_name="country_name",
        hover_data={"region": True, "kg_per_ha": ":.0f", "year": True},
        animation_frame="year",
        color_continuous_scale="YlOrRd",
        range_color=[0, df['kg_per_ha'].quantile(0.95)],  # Fixed scale for comparison
        title="Evolution of Global Fertilizer Consumption (1990-2020)",
        projection="natural earth"
    )
    
    fig.update_layout(
        height=600,
        geo=dict(
            showframe=False,
            showcoastlines=True,
        ),
        sliders=[{
            "currentvalue": {"prefix": "Year: "},
            "pad": {"t": 50}
        }]
    )
    
    # Speed up animation
    fig.layout.updatemenus[0].buttons[0].args[1]["frame"]["duration"] = 500
    
    fig.show()
    return df, fig

# Execute animated version
# animated_df, animated_fig = world_map_with_timeslider()

def interactive_map_with_trends(db_path: str = DB_PATH):
    """Interactive world map with trend chart that updates when countries are clicked."""
    
    con = duckdb.connect(db_path)
    
    # Get data for the map (latest year)
    map_df = con.execute("""
    SELECT 
        iso3,
        country_name,
        region,
        year,
        kg_per_ha
    FROM wb.fertilizer_clean
    WHERE year >= 1990 AND kg_per_ha IS NOT NULL
    ORDER BY year, kg_per_ha DESC
    """).df()
    
    # Create the choropleth map with time slider
    fig_map = px.choropleth(
        map_df,
        locations="iso3",
        color="kg_per_ha", 
        hover_name="country_name",
        hover_data={"region": True, "kg_per_ha": ":.0f", "year": True},
        animation_frame="year",
        color_continuous_scale="YlOrRd",
        range_color=[0, map_df['kg_per_ha'].quantile(0.95)],
        title="🌍 Global Fertilizer Consumption - Click any country to see its trend",
        projection="natural earth"
    )
    
    # Customize the map
    fig_map.update_layout(
        height=500,
        geo=dict(
            showframe=False,
            showcoastlines=True,
        ),
        coloraxis_colorbar=dict(title="kg/ha")
    )
    
    # Create a dummy trend chart (will be updated on click)
    trend_fig = go.Figure()
    trend_fig.add_annotation(
        text="👆 Click on any country in the map to see its fertilizer consumption trend",
        xref="paper", yref="paper",
        x=0.5, y=0.5, xanchor='center', yanchor='middle',
        showarrow=False,
        font=dict(size=16, color="gray")
    )
    trend_fig.update_layout(
        height=400,
        title="Country Trend Analysis",
        xaxis_title="Year",
        yaxis_title="Fertilizer Consumption (kg/ha)",
        showlegend=False
    )
    
    # Display both figures
    fig_map.show()
    trend_fig.show()
    
    # Function to handle country clicks (this would be connected via JavaScript in a full app)
    print("\n💡 In a full web application, we would connect the map clicks to update the trend chart.")
    print("   For now, here's a function to get trend data for any country:")
    
    return map_df, fig_map, trend_fig

def get_country_trend(db_path: str = DB_PATH, country_iso3: str = None, country_name: str = None):
    """Get trend data for a specific country to display when clicked."""
    
    con = duckdb.connect(db_path)
    
    if country_iso3:
        df = con.execute("""
        SELECT 
            country_name,
            year,
            kg_per_ha,
            region
        FROM wb.fertilizer_clean
        WHERE iso3 = ? AND kg_per_ha IS NOT NULL
        ORDER BY year
        """, [country_iso3]).df()
    elif country_name:
        df = con.execute("""
        SELECT 
            country_name,
            year,
            kg_per_ha,
            region
        FROM wb.fertilizer_clean
        WHERE country_name = ? AND kg_per_ha IS NOT NULL
        ORDER BY year
        """, [country_name]).df()
    else:
        con.close()
        return None
    
    con.close()
    
    if df.empty:
        return None
    
    # Create the trend chart
    country_name = df['country_name'].iloc[0]
    region = df['region'].iloc[0]
    
    fig = go.Figure()
    
    # Main trend line
    fig.add_trace(go.Scatter(
        x=df['year'],
        y=df['kg_per_ha'],
        mode='lines+markers',
        name=country_name,
        line=dict(width=3, color='red'),
        marker=dict(size=6)
    ))
    
    # Add some statistics
    max_consumption = df['kg_per_ha'].max()
    min_consumption = df['kg_per_ha'].min()
    latest_consumption = df[df['year'] == df['year'].max()]['kg_per_ha'].iloc[0]
    
    fig.update_layout(
        title=f"📈 Fertilizer Trend: {country_name} ({region})",
        xaxis_title="Year",
        yaxis_title="Fertilizer Consumption (kg/ha)",
        height=400,
        hovermode='x unified',
        annotations=[
            dict(
                x=0.02, y=0.98,
                xref="paper", yref="paper",
                text=f"Peak: {max_consumption:.0f} kg/ha<br>Current: {latest_consumption:.0f} kg/ha",
                showarrow=False,
                bgcolor="white",
                bordercolor="black",
                borderwidth=1
            )
        ]
    )
    
    fig.show()
    return df, fig

# Execute the main dashboard
# map_df, fig_map, trend_fig = interactive_map_with_trends()

# # Example: Show how it would work for specific countries
# print("\n🎯 Examples of country trends:")
# print("Try these countries by running: get_country_trend(country_iso3='USA')")
# print("Or: get_country_trend(country_name='China')")

# # Demonstrate with a few examples
# example_countries = ['USA', 'CHN', 'IND', 'BRA', 'FRA']
# for iso3 in example_countries:
#     result = get_country_trend(country_iso3=iso3)
#     if result is not None:
#         print(f"✓ Loaded trend for {iso3}")