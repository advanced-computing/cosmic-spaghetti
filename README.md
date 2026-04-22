## Group: Cosmic Spaghetti

<a target="_blank" href="https://colab.research.google.com/github/advanced-computing/cosmic-spaghetti">
  <img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Open In Colab"/>
</a>

## Students Names:
- Mery Hotma Situmorang (mhs2231)
- Najihah Fikri (na3183)
 
## Proposal: Housing Affordability & Evictions in New York City

### What dataset are you going to use?
NYC Open Data’s Evictions - We are going to use Evictions data from NYC Open Data. 
This dataset lists executed residential evictions across the five boroughs of New York City since 2017 
and contains detailed information that can be sorted by multiple categories, including court index number, borough etc.
[Link] (https://data.cityofnewyork.us/City-Government/Evictions/6z8x-wfk4/about_data)

ACS Census on Income Data Based on Boroughs – We are planning to use data from the U.S. Census Bureau’s American Community Survey (ACS). 
Specifically, we will use the dataset Median Household Income,aggregated at the county level, which corresponds to New York City’s five boroughs. 
This dataset provides annual estimates of median household income and allows for comparison across boroughs.
However, these datasets are updated every year or every five years.

### What are your research question(s)?
Is there a relationship between borough-level median household income and eviction rates in New York City? 
> We’re looking to explore whether boroughs with lower median household incomes experience higher rates of executed evictions, 
> and whether these patterns vary over time.
Which NYC boroughs have the highest eviction rates (evictions per 1,000 renter households)?
How have eviction rates by borough changed over time since 2017?
Do evictions exhibit seasonal patterns across boroughs (e.g., summer vs. winter spikes)?

### What's the link to your notebook?
https://github.com/advanced-computing/cosmic-spaghetti/blob/main/cosmic-spaghetti-notebook-housing.ipynb

### What's your target visualization?
To answer these questions, we are planning to create the following visualisations:
(1) A choropleth map of New York City boroughs displaying eviction rates per 1,000 renter households, 
allowing for spatial comparison of eviction burden across the city.
(2) A bar chart showing the eviction rate per 1,000 renter households by borough, providing a clear comparison across boroughs.
(3) A line chart illustrating monthly eviction trends by borough, used to identify seasonality and changes over time. 

### What are your known unknowns?
Time to finish the project (we commit to make it an agile project, however we know that there is a deadline of the project)
Other factors that we can explore regarding this issue and whether we can find data to support it



##What challenges do you anticipate?
1. We would need other datasets to help us answer a policy question. For example, once we know that NYC has a housing crisis. 
2. How can we then use the current Evictions data set to help policymakers in making decisions about housing policies and potential housing developments? 
3. Furthermore, how can this dataset be complemented with other datasets on housing in NYC (e.g. renters vs owners, affordable housing developments, etc.). 
Are these datasets available, and are they updated consistently?
4. The existing dataset might be too narrow focused for making a dashboard that giving a broad information 
(we need another aspects of housing to add to make the dashboard a little more complex)
5. We are also considering another project to create a dashboard for the Department of Building NYC Project.
[Link to other Proposal](https://docs.google.com/document/d/1-7YlREUBS8P7rHXURzWfG83eeOqIBWie8olyYbPcFrU/edit?usp=sharing) 
This dashboard might help policymakers plan and make better decisions, such as channeling the appropriate resources to maintain existing buildings and identifying boroughs with high violations.
Policy knowledge regarding the eviction and housing in general

# The App on A Glance
(1) This app contains 3 pages (proposal, building permit and building eviction)
(2) Pages are built by utilizing ```functions``` in ```functions``` page
(3) Data validation and testing can be found in ```tests``` folder

## What this app does

An interactive dashboard exploring NYC building permits and evictions data across the five boroughs. Users can filter by borough, building type, and time period to explore trends and patterns.

# Setup Instruction

## 1. Clone the repo
``` bash
git clone https://github.com/advanced-computing/cosmic-spaghetti.git
cd cosmic-spaghetti 
```


## 2. Create and activate a virtual envirinment
```bash
python -m venv venv
source venv/bin/activate        #Mac/Linux
venv\Scripts\activate           #Windows
```

## 3. Installation
Make sure you install all package by writing 
``` bash
pip install -r requirements.txt
``` 

## 4. Set up secrets
(1) One of the dataset used here is stored in Big Query, you may need to set the ```secrets.toml```
(2) Use instructions [here] (https://github.com/advanced-computing/course-materials/blob/main/docs/project.md)

## 3. Run the streamlit app locally
(1) Now you can run the whole app locally by writing ``` streamlit run streamlit_app.py``` in command line

# Loading Data (team members only)
To refresh the BigQuery tables, you need to authenticate with Google Cloud first:

```bash
gcloud auth application-default login
```

Then run the loading scripts:

```bash
python load_evic_to_bq.py       
python load_permit_to_bq.py     
```

Data is also refreshed automatically every day at 6am UTC via GitHub Actions.

---

# Data Model
Data is pulled from two NYC Open Data APIs and stored in Big Query under `sipa-adv-c-cosmic-spaghetti.cosmic_spaghetti`:

| Table | Source | Method | Frequency |
|---|---|---|---|
| `evictions` | NYC Open Data (`6z8x-wfk4`) | Truncate (full refresh) | Daily |
| `permits` | NYC Open Data (`rbx6-tga4`) | Truncate (last 1 year) | Daily |

**Why Truncate for both?**
- Eviction records get corrected over time — a full refresh ensures accuracy
- Permits are filtered to the last 1 year so the dataset stays manageable
- No reliable unique key is available without BigQuery billing (DML not allowed on free tier)

The Streamlit app reads from BigQuery using a service account key stored in:
- `secrets.toml` locally
- Streamlit Cloud secrets for the deployed app

---

# Performance
Bothe pages load under 2 seconds on subsequent loads using 
`@st.cache_data(ttl=3600)`.
first load takes ~2-3 seconds due to Big Query  cold start latency.

Optimizations made:
- Switched all data reads from the NYC Open Data API to BigQueary 
- Switched all data reads from the NYC Open Data API to BigQuery
- Used `pandas_gbq.read_gbq()` with explicit `dtypes` to speed up type inference
- Added `progress_bar_type=None` to remove tqdm overhead
- Filtered data in SQL (`WHERE`, `IS NOT NULL`, `LIMIT 10000`) rather than in Python
- Used `@st.cache_data` so subsequent page loads are near-instant

---

## Changes based on usability testing (Lecture 10)

During usability testing, participants found the following issues:

- **Missing setup instructions** — the README had no step-by-step guide for running locally
- **No mention of `secrets.toml`** — participants didn't know they needed to create this file
- **No mention of `gcloud` authentication** — the data loading scripts failed without it
- **Service account key not explained** — participants didn't know where to get the key

All of the above have been addressed in this README update.

---

## Running tests
```bash
pytest
```