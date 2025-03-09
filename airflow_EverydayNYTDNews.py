import requests
from datetime import datetime, timedelta
import logging
import os
import time
import pandas as pd

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models.variable import Variable

class EverydayNYTDNews():
    
    def __init__(self):
        # NYT API 설정
        self.API_KEY = Variable.get("NYTD_API_KEY") # ❌Airflow 등록록
        self.BASE_URL = "https://api.nytimes.com/svc/search/v2/articlesearch.json"
        self.news_desks = ["Business", "Financial", "Market Place", "Business Day", "DealBook", "Personal Investing"]
        self.companies = ["Apple", "Amazon", "Google", "Microsoft", "Facebook", "Tesla", "Netflix"]
        self.wait_between_requests = 15
        self.fl_fields = "headline,pub_date,lead_paragraph,web_url,news_desk"
        self.news_desk_fq = 'news_desk:("' + '", "'.join(self.news_desks) + '")'
        
        
        # AWS S3 설정
        self.S3_bucket = "de5-finalproj-team5-test" # ❌수정필요
        self.S3_folder = "raw_data/NYTD"
        self.AWS_CONN_ID = "myaws"
        
    def fetch_page_data(self, url, params, max_retries=5):
        """API 요청 및 429 오류 대비 지수 백오프 적용"""
        retries = 0
        while retries < max_retries:
            try:
                response = requests.get(url, params=params)
                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 429:
                    wait_time = 60 * (2 ** retries)
                    print(f"⚠️ Rate limit hit (429). Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                    retries += 1
                else:
                    print(f"❌ HTTP error {response.status_code} for URL: {response.url}")
                    return None
            except requests.exceptions.RequestException as e:
                print(f"❌ Request error: {e}")
                return None
        return None
        
    def get_data(self, **context):
        all_articles = []
        execution_date = context["execution_date"]
        
        # 날짜(YYYYMMDD 형식)
        # 일일 기사만 가져오는 것으로 변경
        year = execution_date.year
        month = str(execution_date.month).zfill(2)
        begin_date = f"{year}{month}{str(execution_date.day).zfill(2)}"
        end_date = f"{year}{month}{str(execution_date.day).zfill(2)}"
        
        for company in self.companies:
            print(f"📡 Fetching articles for {company}...")
            
            page = 0
            while True:
                params = {
                    "q": company,
                    "fq": self.news_desk_fq,
                    "begin_date": begin_date,
                    "end_date": end_date,
                    "sort": "oldest",
                    "page": page,
                    "fl": self.fl_fields,
                    "facet": "true",
                    "facet_fields": "news_desk",
                    "facet_filter": "true",
                    "api-key": self.API_KEY
                }

                data = self.fetch_page_data(self.BASE_URL, params)
                if not data:
                    break

                docs = data.get("response", {}).get("docs", [])
                if not docs:
                    break
                
                logging.info(f"{company} 데이터 수집 중, 페이지 {page}")
                
                for doc in docs:
                    article = {
                        "headline": doc.get("headline", {}).get("main", ""),
                        "pub_date": doc.get("pub_date", ""),
                        "content": doc.get("lead_paragraph") or doc.get("snippet") or "",
                        "web_url": doc.get("web_url", ""),
                        "news_desk": doc.get("news_desk", "N/A"),
                        "stock": company
                    }
                    all_articles.append(article)
                    
                print(f"Fetched page {page} for {company}, {len(docs)} articles.")
                page += 1
                time.sleep(self.wait_between_requests)
                
            logging.info(f"{company} 데이터 수집 완료")

        if all_articles:
            
            # Parquet 파일 저장
            df = pd.DataFrame(all_articles)
            file_path = f"/opt/airflow/data/nyt_articles_{year}_{month}_{begin_date}.parquet"
            
            # 파일 저장 전에 디렉토리 존재 여부 확인 및 없으면 생성
            os.makedirs(os.path.dirname(file_path), exist_ok=True)

            # 파일 저장
            df.to_parquet(file_path, engine="pyarrow", index=False)
            
            # s3_key 넘겨주기
            # s3_key = f"{self.S3_folder}/{year}/nyt_articles_{year}_{month}_{begin_date}.parquet"
            s3_key = f"raw_data/NYTD/{year}/nyt_articles_{year}_{month}_{begin_date}.parquet"
            
            # XCom에 S3 Key 저장
            context["task_instance"].xcom_push(key="s3_key", value=s3_key)
            context["task_instance"].xcom_push(key="file_path", value= file_path)
        else:
            print("⚠️ No articles found for this date.")

    def s3_write(self, **context):
        s3_hook = S3Hook(aws_conn_id=self.AWS_CONN_ID)
        s3_key = context["task_instance"].xcom_pull(task_ids="task_get_nytd_data", key="s3_key")
        file_path = context["task_instance"].xcom_pull(task_ids="task_get_nytd_data", key="file_path")

        if not s3_key:
            print("⚠️  XCom에서 s3_key를 받지 못함, 업로드하지 않음.")
            return

        # S3에 파일 업로드
        s3_hook.load_file(
            filename=file_path,
            key=s3_key,
            bucket_name=self.S3_bucket,
            replace=True
            )
        logging.info("S3 데이터 업로드 완료")


# 클래스 인스턴스 생성
nyt_news = EverydayNYTDNews()

# DAG 생성
default_args = {
    'owner': 'airflow',
    'depends_on_past': False, # True로?
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'everyday_nytd_s3',
    default_args=default_args,
    start_date=datetime(2025, 3, 1),
    schedule_interval="0 8 * * *",  #매일 한 번씩 오전 8시에 실행(미장 9시 30분 개장)
    catchup=False
) as dag:
    
    task_get_nytd_data = PythonOperator(
        task_id='task_get_nytd_data',
        python_callable=nyt_news.get_data
    )
    
    task_s3_data_write = PythonOperator(
        task_id="task_s3_data_write",
        python_callable=nyt_news.s3_write,
    )
    
    task_get_nytd_data >> task_s3_data_write