import datetime, logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import WriteToText
from apache_beam.io.gcp.bigquery import ReadFromBigQuery, WriteToBigQuery

class FormatGenreId(beam.DoFn):
    def process(self, element):
        record = {'genre_id': element['genre_id'], 'genre': element['genre']}
        return [record]
        
def run():
     PROJECT_ID = 'ilitzkyzhou'
     BUCKET = 'gs://ilitzkyzhou-123'
     DIR_PATH = BUCKET + '/output/' + datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S') + '/'

     options = PipelineOptions(
     flags=None,
     runner='DataflowRunner',
     project=PROJECT_ID,
     job_name='genres',
     temp_location=BUCKET + '/temp',
     region='us-central1')
    
     p = beam.pipeline.Pipeline(options=options)
        
     
     sql = "select GENERATE_UUID() as genre_id, genre from (select distinct genre as genre from datamart.genre_join_dataflow)"
     bq_source = ReadFromBigQuery(query=sql, use_standard_sql=True, gcs_location=BUCKET)
    
     query_results = p | 'Read from BQ' >> beam.io.Read(bq_source)
     
     out_pcoll = query_results | 'Format Genre Id' >> beam.ParDo(FormatGenreId())
     
     out_pcoll | 'Log output' >> WriteToText(DIR_PATH + 'genres_output.txt')
     
     # get appropriate schema id for datamart.genre_dataflow
     dataset_id = 'datamart'
     table_id = PROJECT_ID + ':' + dataset_id + '.' + 'genre_dataflow'
     schema_id = 'genre_id:STRING,genre:STRING'
     
     # writes table to bigquery
     out_pcoll | 'Write to BQ' >> WriteToBigQuery(table=table_id, schema=schema_id, custom_gcs_temp_location=BUCKET)
     
     result = p.run()
     result.wait_until_finish()

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.ERROR)
  run()