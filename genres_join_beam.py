import logging
import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.io.gcp.bigquery import ReadFromBigQuery, WriteToBigQuery

class FormatGenre(beam.DoFn):
    def process(self, element):
        genres = element['genres'].split(',')
        records = []
        for genre in genres:
            # maps each genre to specific title
            record = {'genre': genre, 'tconst': element['tconst']}
            records.append(record)
        
        return records
        

def run():
     PROJECT_ID = 'ilitzkyzhou'
     BUCKET = 'gs://ilitzkyzhou-123/temp'

     options = {
     'project': PROJECT_ID
     }
     opts = beam.pipeline.PipelineOptions(flags=[], **options)

     #DirectRunner PipeLine
     p = beam.Pipeline('DirectRunner', options=opts)

     #selects titles that have ratings
     sql = 'SELECT * from datamart.title where averageRating is not null limit 500'
     bq_source = ReadFromBigQuery(query=sql, use_standard_sql=True, gcs_location=BUCKET)

     # results from above query
     query_results = p | 'Read from BQ' >> beam.io.Read(bq_source)

     # results from applying FormatGenre().process() on every row in the table
     out_pcoll = query_results | 'Format Date' >> beam.ParDo(FormatGenre())

     # outputs result to output.txt
     out_pcoll | 'Log output' >> WriteToText('output.txt')
      
     # get appropriate schema id for datamart.genre_join_beam
     dataset_id = 'datamart'
     table_id = PROJECT_ID + ':' + dataset_id + '.' + 'genre_join_beam'
     schema_id = 'genre:STRING,tconst:STRING'
    
     # writes table to bigquery
     out_pcoll | 'Write to BQ' >> WriteToBigQuery(table=table_id, schema=schema_id, custom_gcs_temp_location=BUCKET)
     
     result = p.run()
     result.wait_until_finish()      

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.ERROR)
  run()