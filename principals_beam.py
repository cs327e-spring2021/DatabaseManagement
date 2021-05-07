import logging
import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.io.gcp.bigquery import ReadFromBigQuery, WriteToBigQuery

class Change_Nothing(beam.DoFn):
    def process(self, element):
        #change literally nothing about the query
        nconst = element['nconst']
        ordering = element['ordering']
        tconst = element['tconst']
        category = element['category']
        job = element['job']
        
        record = {'nconst': nconst, 'ordering': ordering, 'tconst': tconst, 'category': category, 'job': job}
        return [record]

def run():
     PROJECT_ID = 'ilitzkyzhou'
     BUCKET = 'gs://ilitzkyzhou-123/temp'

     options = {
     'project': PROJECT_ID
     }
     opts = beam.pipeline.PipelineOptions(flags=[], **options)

     #DirectRunner PipeLine
     p = beam.Pipeline('DirectRunner', options=opts)

     #sql statement for transformation (selects junctions that refer to a primary key in name_basics_beam)
     sql = 'SELECT p.tconst, p.ordering, p.nconst, p.category, p.job from datamart.principals p join datamart.name_basics_beam n on p.nconst=n.nconst limit 500'
     bq_source = ReadFromBigQuery(query=sql, use_standard_sql=True, gcs_location=BUCKET)

     # results from above query
     query_results = p | 'Read from BQ' >> beam.io.Read(bq_source)

     # results from applying lterally_nothing() on every row in the query
     out_pcoll = query_results | 'Change literally nothing about the table' >> beam.ParDo(Change_Nothing())
    
     # outputs result to output.txt
     out_pcoll | 'Log output' >> WriteToText('output.txt')

     # get appropriate schema id for datamart.principals_beam
     dataset_id = 'datamart'
     table_id = PROJECT_ID + ':' + dataset_id + '.' + 'principals_beam'
     schema_id = 'tconst:STRING,ordering:INT64,nconst:STRING,category:STRING,job:STRING'
    
     # writes table to bigquery
     query_results | 'Write to BQ' >> WriteToBigQuery(table=table_id, schema=schema_id, custom_gcs_temp_location=BUCKET)
     
     result = p.run()
     result.wait_until_finish()      

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.ERROR)
  run()