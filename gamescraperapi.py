from bs4 import BeautifulSoup
import requests
import pandas as pd
from pandas import Series, DataFrame
import psycopg2

def indivgamescraper(url, game_id):
    '''Scrapes date sold and price sold values of the item in url
    
    Args:
        url: link of the item you're wishing to scrape

    Returns:
        (3 dataframes) cleaned dataframes of recently sold loose, cib, and new listings
    '''
    looseurl = url + '#completed-auctions-used'
    ciburl = url + '#completed-auctions-cib'
    newurl = url + '#completed-auctions-new'

    lresult = requests.get(looseurl)
    curl = requests.get(ciburl)
    nurl = requests.get(newurl)


    lc = lresult.content
    cc = curl.content
    nc = nurl.content

    lsoup = BeautifulSoup(lc, features='html.parser')
    csoup = BeautifulSoup(cc, features='html.parser')
    nsoup = BeautifulSoup(nc, features='html.parser')

    ldivid = lsoup.find('div', {'class': 'tab-frame'})
    cdivid = csoup.find('div', {'class': 'tab-frame'})
    ndivid = nsoup.find('div', {'class': 'tab-frame'})

    lsummary = ldivid.find_all('div', {'class':'completed-auctions-used'})
    csummary = cdivid.find_all('div', {'class':'completed-auctions-cib'})
    nsummary = ndivid.find_all('div', {'class':'completed-auctions-new'})

    loose_df = html_cleaning(lsummary, game_id)
    cib_df = html_cleaning(csummary, game_id)
    new_df = html_cleaning(nsummary, game_id)

    return loose_df, cib_df, new_df

def html_cleaning(summary, game_id):
    '''Uses a summary from a BeautifulSoup object to parse information into a DataFrame
    
    Args: 
        summary: Uses the created summary from the html division

    Returns: 
        (dataframe) Cleaned dataframe
    '''

    tables = summary[0].find_all('table')

    data=[]
    rows = tables[0].findAll('tr')

    for tr in rows:
        cols = tr.findAll('td')
        for td in cols:
            texts = td.findAll(text=True)
            for text in texts:
                # print(text)
                data.append(text)


    cleanerdata = []

    for item in data:
        if '\n' not in item:
            cleanerdata.append(item)

    cleandata = []

    for item in cleanerdata:
        if 'Report It' not in item:
            cleandata.append(item)

    index = 0 
    date = []
    price = []
    for item in cleandata:
        if index%2 == 0:
            date.append(item)
        else:
            price.append(item.replace('$',''))
        index +=1

    date = Series(date)
    price = Series(price)

    game_sales_df = pd.concat([date , price], axis=1)
    game_sales_df.columns = ['date','price_sold']
    game_sales_df['game_id'] = game_id

    return game_sales_df


            
def main():
    gs_df = pd.read_csv('game_prices.csv')
    gs_df = DataFrame(gs_df)
    # Data cleaning
    #print(len(gs_df))
    gs_df = gs_df.dropna(subset=['loose_val','complete_val', 'new_val'])
    #print(len(gs_df))

    gs_df['game_url'] = gs_df['game'].str.lower()
    gs_df['game_url'] = gs_df['game_url'].str.replace(' ', '-')
    gs_df['game_url'] = gs_df['game_url'].str.replace("[:\[\].#?]",'',regex=True).str.replace('amp;','')
    #print(gs_df['game'].head(20))
    # Url creation
    console_game_list = list(zip(gs_df['console'], gs_df['game_url']))
    base_pc_url = 'https://www.pricecharting.com/game/'
    url_list = []
    for tpl in console_game_list:
        url = base_pc_url + f'{tpl[0]}' + '/' + f'{tpl[1]}'
        url_list.append(url)
    #print(url_list[:10])
    gs_df['url'] = url_list
    #print(gs_df.head())
    gs_df['game_id'] = range(len(gs_df))
    gs_df['game_id'] += 1
    #print(gs_df.head())


    # Connect to postgresSql
    un = "insert username"
    pw = 'insert password'
    port = 0000 # Insert port number
    db_name = 'insert db_name'

    try:
        # set the connection
        conn = psycopg2.connect(dbname=db_name, user=un, password=pw, port=port, host='localhost')
        # Initialize the cursor
        cursor = conn.cursor()
        # cursor execute to query
        cursor.execute('SELECT version();')
        result = cursor.fetchone()
        print("The version of PostgreSQL is:", result)

# Creation of the table for Avg game prices

        query = '''
                    DROP TABLE IF EXISTS avg_game_prices;
                    CREATE TABLE avg_game_prices
                    (game_id integer PRIMARY KEY,
                    console varchar,
                    loose_val money,
                    complete_val money,
                    new_val money,
                    date_scraped varchar,
                    game_url varchar,
                    url varchar );
                '''
        
        cursor.execute(query)

        print('The avg_game_prices table has been created successfully')

        values = zip(gs_df['game_id'], gs_df["console"], gs_df["loose_val"], gs_df["complete_val"], gs_df["new_val"], gs_df["date(D/M/Y)"], gs_df["game_url"], gs_df["url"])

        #for i in range(len(gs_df)):
        #    values = zip(gs_df['game_id'][i], gs_df["console"][i], gs_df["loose_val"][i], gs_df["complete_val"][i], gs_df["new_val"][i], gs_df["date(D/M/Y)"][i], gs_df["game_url"][i], gs_df["url"][i])
        query = '''
                INSERT INTO
                avg_game_prices (game_id, console, loose_val, complete_val, new_val, date_scraped, game_url, url)
                VALUES (%s, %s, %s, %s, %s, %s, %s,%s)
                ;
                '''
        cursor.executemany(query, values)
        print('Inserted records into avg_game_prices')


# Creation of the tables for recent sales of each of the games

        query = '''
                DROP TABLE IF EXISTS loose_game_prices;
                CREATE TABLE loose_game_prices
                (game_id integer,
                date_sold varchar,
                price_sold money,
                FOREIGN KEY (game_id) REFERENCES avg_game_prices (game_id)
                );                
                '''
        cursor.execute(query)
        print('The loose_game_prices table has been created successfully')

        query = '''
                DROP TABLE IF EXISTS cib_game_prices;
                CREATE TABLE cib_game_prices
                (game_id integer,
                date_sold varchar,
                price_sold money,
                FOREIGN KEY (game_id) REFERENCES avg_game_prices (game_id)
                );                
                '''
        cursor.execute(query)
        print('The cib_game_prices table has been created successfully')

        query = '''
                DROP TABLE IF EXISTS new_game_prices;
                CREATE TABLE new_game_prices
                (game_id integer,
                date_sold varchar,
                price_sold money,
                FOREIGN KEY (game_id) REFERENCES avg_game_prices (game_id)
                );                
                '''
        cursor.execute(query)
        print('The new_game_prices table has been created successfully')        

        loose_df = pd.DataFrame(columns=['date', 'price_sold', 'game_id'])
        cib_df = pd.DataFrame(columns=['date', 'price_sold', 'game_id'])
        new_df = pd.DataFrame(columns=['date', 'price_sold', 'game_id'])
        for i in range(3):
            url = gs_df['url'][i]
            id = gs_df['game_id'][i]
            l_df, c_df, n_df = indivgamescraper(url, id)
            loose_df = pd.concat([loose_df, l_df])
            cib_df = pd.concat([cib_df, c_df])
            new_df = pd.concat([new_df, n_df])
        print(loose_df.head(), '\n',cib_df.head(), '\n', new_df.head())

        #make the changes to the database persistent
        conn.commit()



    except psycopg2.Error as e:
        print('Fail to execute due to the error:', e)

    cursor.close()
if __name__ == "__main__":
    print('Starting Pricecharting API process\n')
    main()
    print('Finished Pricecharting API process')
