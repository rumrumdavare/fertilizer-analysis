import duckdb
import plotly.graph_objects as go
import streamlit as st
from datetime import datetime
from fertilizer_sql_analysis import (
    load_api_to_duckdb,
    clean_with_sql, 
    visualize_top_consumers_2020,
    visualize_trend_line_chart,
    peak_consumption_advanced_interactive,
    consumption_change_analysis,
    world_map_with_timeslider,
    interactive_map_with_trends,
    get_country_trend
)

DB_PATH = "fertilizer.duckdb"

# Add caching decorators for expensive operations
@st.cache_data(ttl=3600)
def cached_load_fertilizer_data():
    """Cache the data loading to avoid repeated database queries"""
    con = duckdb.connect(DB_PATH)
    df = con.execute("""
        SELECT iso3, country_name, region, year, kg_per_ha
        FROM wb.fertilizer_clean 
        WHERE kg_per_ha IS NOT NULL
        ORDER BY year, country_name
    """).df()
    con.close()
    return df

@st.cache_data
def cached_country_list():
    """Cache the list of available countries"""
    con = duckdb.connect(DB_PATH)
    countries = con.execute("""
        SELECT DISTINCT country_name 
        FROM wb.fertilizer_clean 
        ORDER BY country_name
    """).df()['country_name'].tolist()
    con.close()
    return countries

@st.cache_data
def cached_region_list():
    """Cache the list of available regions"""
    con = duckdb.connect(DB_PATH)
    regions = con.execute("""
        SELECT DISTINCT region 
        FROM wb.fertilizer_clean 
        WHERE region IS NOT NULL
        ORDER BY region
    """).df()['region'].tolist()
    con.close()
    return regions

def initialize_session_state():
    """Initialize all session state variables"""
    if 'selected_country' not in st.session_state:
        st.session_state.selected_country = None
    if 'last_etl_run' not in st.session_state:
        st.session_state.last_etl_run = None
    if 'cached_data' not in st.session_state:
        st.session_state.cached_data = None

def optimized_show_overview_dashboard(year_range, selected_region):
    """Optimized overview dashboard with cached data"""
    st.header("📊 Overview Dashboard")
    
    # Load data with caching
    if st.session_state.cached_data is None:
        with st.spinner("Loading data..."):
            st.session_state.cached_data = cached_load_fertilizer_data()
    
    df = st.session_state.cached_data
    
    # Filter data based on selections
    filtered_df = df[
        (df['year'] >= year_range[0]) & 
        (df['year'] <= year_range[1])
    ]
    
    if selected_region != "All Regions":
        filtered_df = filtered_df[filtered_df['region'] == selected_region]
    
    # Key metrics
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        total_countries = filtered_df['country_name'].nunique()
        st.metric("Countries", total_countries)
    with col2:
        avg_consumption = filtered_df['kg_per_ha'].mean()
        st.metric("Avg Consumption", f"{avg_consumption:.0f} kg/ha")
    with col3:
        max_consumption = filtered_df['kg_per_ha'].max()
        st.metric("Peak Consumption", f"{max_consumption:.0f} kg/ha")
    with col4:
        data_points = len(filtered_df)
        st.metric("Data Points", f"{data_points:,}")
    
    # Use your existing function for top consumers
    st.subheader("Top Fertilizer Consumers (Latest Data)")
    top_df = visualize_top_consumers_2020(top_n=15)
    st.dataframe(
        top_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "country_name": "Country",
            "region": "Region", 
            "kg_per_ha": st.column_config.NumberColumn("Consumption (kg/ha)", format="%d kg/ha")
        }
    )
    
    # Peak consumption interactive chart
    st.subheader("Peak Consumption Analysis")
    peak_df, peak_fig = peak_consumption_advanced_interactive(top_n=20)
    st.plotly_chart(peak_fig, use_container_width=True)

def show_country_trends(year_range, selected_region):
    st.header("📈 Country Trend Analysis")
    
    # Country selector with cached list
    available_countries = cached_country_list()
    
    selected_countries = st.multiselect(
        "Select Countries to Compare",
        available_countries,
        default=["China", "India", "United States", "Brazil"][:3],
        max_selections=6
    )
    
    if selected_countries:
        # Use your existing trend function
        trend_df = visualize_trend_line_chart(
            countries=selected_countries,
            year_start=year_range[0],
            year_end=year_range[1]
        )
        
        # Country comparison metrics
        st.subheader("Country Comparison")
        cols = st.columns(len(selected_countries))
        for idx, country in enumerate(selected_countries):
            country_data = trend_df[trend_df['country_name'] == country]
            if not country_data.empty:
                latest = country_data[country_data['year'] == country_data['year'].max()]['kg_per_ha'].iloc[0]
                with cols[idx]:
                    st.metric(country, f"{latest:,.0f} kg/ha")

def show_world_map(year_range, selected_region):
    st.header("🌐 World Fertilizer Consumption")
    
    # Use your existing world map function
    st.info("Interactive world map with time slider - use the play button to see trends over time")
    map_df, map_fig = world_map_with_timeslider()
    st.plotly_chart(map_fig, use_container_width=True)
    
    # Map statistics
    st.subheader("Global Statistics")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Countries with Data", f"{len(map_df['iso3'].unique()):,}")
    with col2:
        if not map_df.empty:
            latest_year = map_df['year'].max()
            latest_data = map_df[map_df['year'] == latest_year]
            if not latest_data.empty:
                max_country = latest_data.nlargest(1, 'kg_per_ha')['country_name'].iloc[0]
                st.metric("Highest Consumer", max_country)
    with col3:
        if not map_df.empty:
            st.metric("Global Average", f"{map_df['kg_per_ha'].mean():.0f} kg/ha")

def enhanced_change_analysis(year_range, selected_region):
    """Enhanced change analysis with interactive controls"""
    st.header("⚡ Consumption Change Analysis")
    
    # Analysis configuration
    col1, col2 = st.columns(2)
    with col1:
        change_start = st.selectbox("Start Year", options=list(range(1990, 2021)), index=10)
    with col2:
        change_end = st.selectbox("End Year", options=list(range(1990, 2024)), index=30)
    
    # Additional filters
    show_only_significant = st.checkbox("Show only significant changes (±50 kg/ha)")
    min_change_threshold = st.slider("Minimum Change Threshold (kg/ha)", 0, 200, 50)
    
    if st.button("🔍 Analyze Changes", type="primary"):
        with st.spinner("Calculating consumption changes..."):
            # Use your existing change analysis function
            change_df = consumption_change_analysis(
                year_start=change_start,
                year_end=change_end
            )
            
            # Apply filters
            if show_only_significant:
                change_df = change_df[abs(change_df['absolute_change']) >= min_change_threshold]
            
            display_change_analysis_results(change_df, change_start, change_end)

def display_change_analysis_results(change_df, start_year, end_year):
    """Display the results of change analysis"""
    if change_df.empty:
        st.warning("No data matches your criteria")
        return
    
    # Top increases and decreases
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📈 Largest Increases")
        increases = change_df[change_df['absolute_change'] > 0].head(10)
        for idx, row in increases.iterrows():
            with st.container():
                col_a, col_b = st.columns([2, 1])
                with col_a:
                    st.write(f"**{row['country_name']}**")
                    st.caption(f"{row['region']}")
                with col_b:
                    st.metric("", f"+{row['absolute_change']:.0f}", delta=f"+{row['percent_change']:.1f}%")
                st.progress(min(row['percent_change'] / 100, 1.0))
    
    with col2:
        st.subheader("📉 Largest Decreases")
        decreases = change_df[change_df['absolute_change'] < 0].head(10)
        for idx, row in decreases.iterrows():
            with st.container():
                col_a, col_b = st.columns([2, 1])
                with col_a:
                    st.write(f"**{row['country_name']}**")
                    st.caption(f"{row['region']}")
                with col_b:
                    st.metric("", f"{row['absolute_change']:.0f}", delta=f"{row['percent_change']:.1f}%")
                st.progress(min(abs(row['percent_change']) / 100, 1.0))

def optimized_data_management():
    """Enhanced data management with progress tracking"""
    st.header("🔄 Data Management")
    
    # Database status
    st.subheader("Database Status")
    con = duckdb.connect(DB_PATH)
    stats = con.execute("""
        SELECT 
            COUNT(*) as total_records,
            MIN(year) as first_year,
            MAX(year) as last_year,
            COUNT(DISTINCT country_name) as countries,
            COUNT(DISTINCT region) as regions
        FROM wb.fertilizer_clean
    """).fetchone()
    con.close()
    
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Records", f"{stats[0]:,}")
    col2.metric("Countries", stats[3])
    col3.metric("Time Span", f"{stats[1]} - {stats[2]}")
    col4.metric("Regions", stats[4])
    
    # Data operations
    st.subheader("Data Operations")
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("🔄 Run Full ETL Pipeline", type="primary", use_container_width=True):
            with st.spinner("Downloading data from World Bank API..."):
                try:
                    load_api_to_duckdb()
                    clean_with_sql()
                    st.session_state.last_etl_run = datetime.now()
                    st.session_state.cached_data = None  # Clear cache
                    st.success("✅ ETL pipeline completed successfully!")
                    st.rerun()
                except Exception as e:
                    st.error(f"❌ ETL failed: {str(e)}")
    
    with col2:
        if st.button("🗑️ Clear Cache", use_container_width=True):
            st.session_state.cached_data = None
            st.cache_data.clear()
            st.success("✅ Cache cleared!")
    
    # Last ETL run info
    if st.session_state.last_etl_run:
        st.info(f"Last ETL run: {st.session_state.last_etl_run.strftime('%Y-%m-%d %H:%M:%S')}")

def add_footer():
    """Add a professional footer to the app"""
    st.markdown("---")
    col1, col2, col3 = st.columns([2, 1, 1])
    with col1:
        st.caption("🌍 Global Fertilizer Consumption Analysis")
        st.caption("Data source: World Bank API | Built with Streamlit")
    with col2:
        st.caption(f"Last update: {datetime.now().strftime('%Y-%m-%d')}")
    with col3:
        if st.session_state.cached_data is not None:
            record_count = len(st.session_state.cached_data)
            st.caption(f"📊 {record_count:,} records loaded")

def main():
    st.set_page_config(
        page_title="Fertilizer Consumption Analysis",
        page_icon="🌾",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    # Initialize session state
    initialize_session_state()
    
    st.title("🌍 Global Fertilizer Consumption Analysis")
    st.markdown("Analyzing fertilizer use patterns across countries and time")
    
    # Sidebar navigation
    st.sidebar.title("Navigation")
    app_mode = st.sidebar.selectbox(
        "Choose Analysis Type",
        [
            "📊 Overview Dashboard",
            "🌐 World Map", 
            "📈 Country Trends",
            "⚡ Change Analysis",
            "🔄 Data Management"
        ]
    )
    
    # Global filters
    st.sidebar.markdown("---")
    st.sidebar.subheader("Global Filters")
    
    year_range = st.sidebar.slider(
        "Select Year Range",
        min_value=1990,
        max_value=2023,
        value=(2000, 2020)
    )
    
    # Use cached region list
    all_regions = ["All Regions"] + cached_region_list()
    selected_region = st.sidebar.selectbox("Filter by Region", all_regions)
    
    # Performance info
    st.sidebar.markdown("---")
    st.sidebar.caption("⚡ Performance Optimized")
    if st.session_state.cached_data is not None:
        st.sidebar.caption(f"📁 {len(st.session_state.cached_data):,} records cached")
    
    # Route to appropriate function
    if app_mode == "📊 Overview Dashboard":
        optimized_show_overview_dashboard(year_range, selected_region)
    elif app_mode == "🌐 World Map":
        show_world_map(year_range, selected_region)
    elif app_mode == "📈 Country Trends":
        show_country_trends(year_range, selected_region)
    elif app_mode == "⚡ Change Analysis":
        enhanced_change_analysis(year_range, selected_region)
    elif app_mode == "🔄 Data Management":
        optimized_data_management()
    
    # Add footer
    add_footer()

if __name__ == "__main__":
    main()