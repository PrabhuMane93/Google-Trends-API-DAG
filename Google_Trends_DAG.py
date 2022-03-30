# Google_Trends_DAG.py

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
#pip install pytrends
import pytrends
import pandas as pd
from datetime import datetime
from pytrends.request import TrendReq


default_args = {
    'start_date': days_ago(1),
}

@dag(schedule_interval='@daily', default_args=default_args, catchup=False)
def taskflow():
    keyword = [AAPL]
    @task
    def trends(location):
        pytrends = TrendReq(hl='en-IN', tz=330, geo='IN', )
        """
        hl : host language for accessing Google Trends
        tz : Timezone Offset (in minutes), IST 
        geo : Two letter country abbreviation
        """
        pytrends.build_payload(keyword, cat = 0 , timeframe = 'now 1-d' , geo = location , gprop='')
        """
        build_payload method: A payload in API is the actual data pack that is sent and 
        is the crucial information that you submit to the server when you are making an API request, 
        build method helps in creating payload request to the API server.
        Parameters:
        keyword : list containing keyword (keyword is the stockticker in this case)
        cat : Category to narrow results
        gprop : What Google property to filter to
        """
        data = pytrends.interest_over_time()
        """
        The information that we get from interest_over_time method are wide time ranges which we had selected in our timeframe
        So to eliminate those ranges, we perform mean() on our data object(since data object stores the requested API information)
        """
        mean = data.mean()
        mean = mean.tolist()  
        return mean

    ### Defining Checker Function
    @task
    def checker():
        now = datetime.now()
        a=0
        Google_Trends = pd.read_csv('E:\Internship\Phoenixgen\Dataloader\Google Trends\Google_Trends.csv')
        for b in Google_Trends['Date']:
            if b == now.strftime("%d/%m/%Y"):
                a=a+1
        return a

    """## **STEP 2**

    ### Function for Adding today's google trend searches to the csv file
    """
    @task
    def update_dataset():
        check = checker()
        Google_Trends = pd.read_csv('E:\Internship\Phoenixgen\Dataloader\Google Trends\Google_Trends.csv')
        if (check!=0):
            str1 = 'You have already extracted data today for '
            str2 = keyword[0]
            print(str1+str2)
        elif (check==0):
            j=0
            now = datetime.now()
            region = ['IN','US','GB','AU','JP']  
            for i in region:
                Google_Trends.loc[len(Google_Trends.index)] = [keyword[0], now.strftime("%d/%m/%Y"), trends(region[j])[0], region[j]]
                j=j+1
        print(Google_Trends)
        Google_Trends.to_csv('E:\Internship\Phoenixgen\Dataloader\Google Trends\Google_Trends.csv', index=False)

    """Summoning the Functions and Outputing the Dataframe"""
    update_dataset()

dag = taskflow()