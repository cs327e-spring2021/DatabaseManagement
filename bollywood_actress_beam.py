import logging
import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.io.gcp.bigquery import ReadFromBigQuery, WriteToBigQuery

class FormatName(beam.DoFn):
    def process(self, element):
        name = element['Name'].split(' ')
        # get first name of the record
        first_name = name[0]
        #get last name of record (if exists)
        last_name = '\\N'
        if len(name) > 1:
            last_name = name[1]
        height = element['Height_in_cm_']
        lead = element['Debut_aslead_role']
        record = {'firstName': first_name, 'lastName': last_name, 'Height_in_cm_': height, 'Debut_aslead_role': lead}
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

     #we only want the actresses that reference a valid primary key in the bollywood_beam table to preserve referential integrity
     sql = 'SELECT a.Name, a.Height_in_cm_, a.Debut_aslead_role from datamart.bollywood_actress a join datamart.bollywood_beam b on b.Title=a.Debut_aslead_role limit 500'
     bq_source = ReadFromBigQuery(query=sql, use_standard_sql=True, gcs_location=BUCKET)

    # results from above query
     query_results = p | 'Read from BQ' >> beam.io.Read(bq_source)

    # results from applying FormatName().process() on every row in the table
     out_pcoll = query_results | 'Format Name' >> beam.ParDo(FormatName())

    # outputs result to output.txt
     out_pcoll | 'Log output' >> WriteToText('output.txt')

    # get appropriate schema id for datamart.bollywood_actress_beam
     dataset_id = 'datamart'
     table_id = PROJECT_ID + ':' + dataset_id + '.' + 'bollywood_actress_beam'
     schema_id = 'firstName:STRING,lastName:STRING,Height_in_cm_:INT64,Debut_aslead_role:STRING'

    # writes table to bigquery
     out_pcoll | 'Write to BQ' >> WriteToBigQuery(table=table_id, schema=schema_id, custom_gcs_temp_location=BUCKET)
     
     result = p.run()
     result.wait_until_finish()      

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.ERROR)
  run()