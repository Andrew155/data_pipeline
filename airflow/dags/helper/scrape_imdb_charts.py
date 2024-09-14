from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from google.cloud import bigquery
import datetime
import pandas as pd
import os
from bs4 import BeautifulSoup

# Path to Chrome driver
chrome_driver_path = '"/usr/local/bin/chromedriver"'

# ChromeOptions
chrome_options = Options()
chrome_options.add_argument('--headless')  # Chạy chế độ headless để không hiển thị giao diện trình duyệt

# WebDriver
service = Service(chrome_driver_path)
driver = webdriver.Chrome(service=service, options=chrome_options)

# Setting headers
driver.execute_cdp_cmd('Network.setUserAgentOverride', {
    'userAgent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36',
    'acceptLanguage': 'en-US,en;q=0.9'
})


custom_headers = {
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1'
}

# Key Google set-up
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/Users/nguyenlongnhat/DE_Project/airflow/dags/configs/ServiceKey_GoogleCloud.json'
# Big query connect
bigquery_client = bigquery.Client()

def _get_soup(chart):
    '''
    Get the page source from a url using Selenium.
    Args:
        - chart(str) = chart to scrape
            Options: 'most_popular_movies', 'top_250_movies', 'top_english_movies', 'top_250_tv'
    Returns:
        - page_source(str) = HTML page source
    '''

    urls = {
        'most_popular_movies': 'https://www.imdb.com/chart/moviemeter?pf_rd_m=A2FGELUUNOQJNL&pf_rd_p=470df400-70d9-4f35-bb05-8646a1195842&pf_rd_r=5V6VAGPEK222QB9E0SZ8&pf_rd_s=right-4&pf_rd_t=15506&pf_rd_i=toptv&ref_=chttvtp_ql_2',
        'top_250_movies': 'https://www.imdb.com/chart/top?pf_rd_m=A2FGELUUNOQJNL&pf_rd_p=470df400-70d9-4f35-bb05-8646a1195842&pf_rd_r=5V6VAGPEK222QB9E0SZ8&pf_rd_s=right-4&pf_rd_t=15506&pf_rd_i=toptv&ref_=chttvtp_ql_3',
        'top_english_movies': 'https://www.imdb.com/chart/top-english-movies?pf_rd_m=A2FGELUUNOQJNL&pf_rd_p=470df400-70d9-4f35-bb05-8646a1195842&pf_rd_r=3YMHR1ECWH2NNG5TPH1C&pf_rd_s=right-4&pf_rd_t=15506&pf_rd_i=boxoffice&ref_=chtbo_ql_4',
        'top_250_tv': 'https://www.imdb.com/chart/tvmeter?pf_rd_m=A2FGELUUNOQJNL&pf_rd_p=470df400-70d9-4f35-bb05-8646a1195842&pf_rd_r=J9H259QR55SJJ93K51B2&pf_rd_s=right-4&pf_rd_t=15506&pf_rd_i=topenglish&ref_=chttentp_ql_5'
    }

    url = urls.get(chart)
    if not url:
        raise ValueError("Invalid chart option")

    driver.get(url)

    # Wait for the necessary elements to be present
    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.CLASS_NAME, 'ipc-metadata-list-summary-item'))
    )

    page_source = driver.page_source
    return page_source

def _scrape_movies(page_source):
    '''
    Scrape the most popular titles and ratings from the IMDB website.
    Args:
        - page_source(str) = HTML page source
    Returns:
        - movie_df(DataFrame) = DataFrame of movie names, ratings, and user votings
    '''
    soup = BeautifulSoup(page_source, 'html.parser')
    
    movie_names = []
    movie_years = []
    movie_ratings = []
    user_votings = []

    # Locate movie entries in the HTML
    movie_entries = soup.find_all('li', {'class': 'ipc-metadata-list-summary-item'})
    
    print(f"Found {len(movie_entries)} movie entries on the page.")

    for entry in movie_entries:
        # Extract movie title
        title_tag = entry.find('h3', {'class': 'ipc-title__text'})
        if title_tag:
            movie_names.append(title_tag.text)
        else:
            print('Missing title. Replacing with -1')
            movie_names.append(-1)

        # Extract movie year
        year_tag = entry.find('span', {'class': 'sc-b189961a-8'})
        if year_tag:
            try:
                year_text = year_tag.text
                year = int(year_text.split('–')[0].strip())
                movie_years.append(year)
            except ValueError:
                print('Missing year. Replacing with -1')
                movie_years.append(-1)
        else:
            print('Missing year. Replacing with -1')
            movie_years.append(-1)

        # Extract movie rating
        rating_tag = entry.find('span', {'class': 'ipc-rating-star--rating'})
        if rating_tag:
            try:
                rating = float(rating_tag.text)
                movie_ratings.append(rating)
            except ValueError:
                print('Missing rating. Replacing with -1')
                movie_ratings.append(-1)
        else:
            print('Missing rating. Replacing with -1')
            movie_ratings.append(-1)

        # Extract user votes
        votes_tag = entry.find('span', {'class': 'ipc-rating-star--voteCount'})
        if votes_tag:
            try:
                votes_str = votes_tag.text.strip().replace(',', '')
                user_votings.append(votes_str[1:-1])
            except ValueError:
                print('Missing user votes. Replacing with -1')
                user_votings.append(-1)
        else:
            print('Missing user votes. Replacing with -1')
            user_votings.append(-1)

    # Create a dataframe
    movie_df = pd.DataFrame({
        'movie_name': movie_names, 
        'movie_year': movie_years, 
        'movie_rating': movie_ratings, 
        'user_votings': user_votings
    })

    # Add movie_id
    movie_df['movie_id'] = movie_df.index + 1

    # Set date
    movie_df['update_date'] = datetime.datetime.today().strftime('%Y-%m-%d')

    # Reorder columns
    movie_df = movie_df[['movie_id', 'movie_name', 'movie_year', 'movie_rating', 'user_votings', 'update_date']]

    return movie_df
# Create a dataset called test_dataset
def _getOrCreate_dataset(dataset_name :str) -> bigquery.dataset.Dataset:

    '''
    Get dataset. If the dataset does not exists, create it.
    
    Args:
        - dataset_name(str) = Name of the new/existing data set.
        - project_id(str) = project id(default = The project id of the bigquery_client object)

    Returns:
        - dataset(google.cloud.bigquery.dataset.Dataset) = Google BigQuery Dataset
    '''

    print('Fetching Dataset...')

    try:
        # get and return dataset if exist
        dataset = bigquery_client.get_dataset(dataset_name)
        print('Done')
        print(dataset.self_link)
        return dataset

    except Exception as e:
        # If not, create and return dataset
        if e.code == 404:
            print('Dataset does not exist. Creating a new one.')
            bigquery_client.create_dataset(dataset_name)
            dataset = bigquery_client.get_dataset(dataset_name)
            print('Done')
            print(dataset.self_link)
            return dataset
        else:
            print(e)
def _getOrCreate_table(dataset_name:str, table_name:str) -> bigquery.table.Table:


    '''
    Create a table. If the table already exists, return it.
    
    Args:
        - table_name(str) = Name of the new/existing table.
        - dataset_name(str) = Name of the new/existing data set.
        - project_id(str) = project id(default = The project id of the bigquery_client object)

    Returns:
        - table(google.cloud.bigquery.table.Table) = Google BigQuery table
    '''

    # Grab prerequisites for creating a table
    dataset = _getOrCreate_dataset(dataset_name)
    project = dataset.project
    dataset = dataset.dataset_id
    table_id = project + '.' + dataset + '.' + table_name

    print('\nFetching Table...')

    try:
        # Get table if exists
        table = bigquery_client.get_table(table_id)
        print('Done')
        print(table.self_link)
    except Exception as e:

        # If not, create and get table
        if e.code == 404:
            print('Table does not exist. Creating a new one.')
            bigquery_client.create_table(table_id)
            table = bigquery_client.get_table(table_id)
            print(table.self_link)
        else:
            print(e)
    finally:
        return table
def _load_to_bigQuery(movie_names, chart, dataset_name='imdb'):

    '''
    Load data into BigQuery table.
    Args:
        - movie_names(pd.DataFrame) = dataframe of movie names
        - chart(str) = Name of the chart
            Options: most_popular_movies, top_250_movies, top_english_movies, top_250_tv
        - dataset_name(str) = Name of the new/existing data set.
        - project_id(str) = project id(default = The project id of the bigquery_client object)
        - date_to_load(datetime.datetime) = Date to load into the table
    Returns:
        - None
    Notes:
        - The function will create a new dataset and table if they do not exist.
        - The function will overwrite the table if it already exists.
    '''

    if chart == 'most_popular_movies':
       table_name = 'most_popular_movies'
    
    if chart == 'top_250_movies':
        table_name = 'top_250_movies'

    if chart == 'top_english_movies':
        table_name = 'top_english_movies'

    if chart == 'top_250_tv':
        table_name = 'top_250_tv'
    # Create a table
    table = _getOrCreate_table(dataset_name, table_name)

    # Create a job config
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        schema=[
            bigquery.SchemaField("movie_id", bigquery.enums.SqlTypeNames.INT64),
            bigquery.SchemaField("movie_name", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("movie_year", bigquery.enums.SqlTypeNames.INT64),
            bigquery.SchemaField("movie_rating", bigquery.enums.SqlTypeNames.FLOAT64),
            bigquery.SchemaField("user_votings", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("update_date", bigquery.enums.SqlTypeNames.DATE),
        ],
        write_disposition="WRITE_TRUNCATE",
    )

    # Load data into the table
    job = bigquery_client.load_table_from_dataframe(
        movie_names, table, job_config=job_config
    )


    job.result()


    print("Loaded {} rows into {}:{}.".format(job.output_rows, dataset_name, table_name))

def main():
    soup = _get_soup(chart='top_250_movies')
    movies_df = _scrape_movies(soup)
    print(f"Found {len(movies_df)} movie entries.")
    _load_to_bigQuery(movies_df, chart='top_250_movies')

if __name__ == '__main__':
    main()
