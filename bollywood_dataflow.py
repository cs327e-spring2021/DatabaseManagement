import datetime, logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import WriteToText
from apache_beam.io.gcp.bigquery import ReadFromBigQuery, WriteToBigQuery

class FormatDate(beam.DoFn):
    def process(self, element):
        # movie year
        year = element['Year']
        title =element['Title']
        director = element['Director']
        
        # numerical form of month
        month = element['Release_Month']
        release_month = None
        if month=='JAN':
            release_month = 1
        elif month=='FEB':
            release_month = 2
        elif month=='MAR':
            release_month = 3
        elif month=='APR':
            release_month = 4
        elif month=='MAY':
            release_month = 5
        elif month=='JUN':
            release_month = 6
        elif month=='JUL':
            release_month = 7
        elif month=='AUG':
            release_month = 8
        elif month=='SEP':
            release_month = 9
        elif month=='OCT':
            release_month = 10
        elif month=='NOV':
            release_month = 11
        elif month=='DEC':
            release_month = 12
        #easier to manage chronological release order
        if release_month is not None:
            Numerical_Date = year * 365 + (release_month - 1) * 30 + element['Release_Date']
            release_date = str(year) + '-' + str(release_month) + '-' + str(element['Release_Date'])
        else:
            Numerical_Date = None
            release_date = None
        
        record = {'Title': title, 'Director': director, 'Release_Month': release_month, 'Release_Date': release_date, 'Numerical_Date': Numerical_Date}
        return [record]

def run():
     PROJECT_ID = 'ilitzkyzhou'
     BUCKET = 'gs://ilitzkyzhou-123'
     DIR_PATH = BUCKET + '/output/' + datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S') + '/'

     options = PipelineOptions(
     flags=None,
     runner='DataflowRunner',
     project=PROJECT_ID,
     job_name='bollywood',
     temp_location=BUCKET + '/temp',
     region='us-central1')

     #DirectRunner PipeLine
     p = beam.pipeline.Pipeline(options=options)

     #sql statement for transformation (selects all movies)
     sql = 'SELECT * from datamart.bollywood'
     bq_source = ReadFromBigQuery(query=sql, use_standard_sql=True, gcs_location=BUCKET)

     # results from above query
     query_results = p | 'Read from BQ' >> beam.io.Read(bq_source)

     # results from applying FormatName().process() on every row in the table
     out_pcoll = query_results | 'Format Date' >> beam.ParDo(FormatDate())

     # outputs result to output.txt
     out_pcoll | 'Log output' >> WriteToText('output.txt')

     # get appropriate schema id for datamart.bollywood_dataflow
     dataset_id = 'datamart'
     table_id = PROJECT_ID + ':' + dataset_id + '.' + 'bollywood_dataflow'
     schema_id = 'Title:STRING,Director:STRING,Release_Month:INT64,Release_Date:DATE,Numerical_Date:INT64'
    
     # writes table to bigquery
     out_pcoll | 'Write to BQ' >> WriteToBigQuery(table=table_id, schema=schema_id, custom_gcs_temp_location=BUCKET)
     
     result = p.run()
     result.wait_until_finish()      

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.ERROR)
  run()