import logging, os, datetime
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

# DoFn to perform on each element in the input PCollection.
class StateName(beam.DoFn):
  def process(self, element):
    crime_record = element
    crime_state = crime_record.get('agency_code')
    crime_year = crime_record.get('report_year')
    crime_num = crime_record.get('rapes')
    crime_percapita = crime_record.get('rapes_percapita')
    state_abb = crime_state[0:2]
    result = {"report_year":crime_year,"state":state_abb,"number_of_crimes": crime_num, "crime_percapita":crime_percapita}
    return [result]

PROJECT_ID = os.environ['PROJECT_ID']
BUCKET = os.environ['BUCKET']
DIR_PATH = BUCKET + '/output/' + datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S') + '/'

# Project ID is needed for BigQuery data source, even for local execution.
options = {
    'runner': 'DataflowRunner',
    'job_name': 'transform-state-rapes',
    'project': PROJECT_ID,
    'temp_location': BUCKET + '/temp',
    'staging_location': BUCKET + '/staging',
    'machine_type': 'n1-standard-8',
    'num_workers': 8
}
opts = beam.pipeline.PipelineOptions(flags=[], **options)

with beam.Pipeline('DataflowRunner', options=opts) as p:
    
    query_results = p | beam.io.Read(beam.io.BigQuerySource(query='SELECT report_year, agency_code,rapes,rapes_percapita FROM crime_rates.crime_rapes'))

    # write PCollection to a log file
    query_results | 'Write to File 1' >> WriteToText(DIR_PATH+'rapes_query.txt')

    #apply Pardo on the Pcollection
    state_pcoll = query_results | 'Create State abb' >> beam.ParDo(StateName())
    
    # write PCollection to a file
    state_pcoll | 'Write to File 2' >> WriteToText(DIR_PATH+'output_rapes.txt')
    
    qualified_takes_table_name = 'han97jiayan:crime_rates.rapes_transfrom_cluster'
    takes_table_schema = 'report_year:INTEGER,state:STRING,number_of_crimes:INTEGER,crime_percapita:FLOAT'
    
    state_pcoll | 'Write Takes to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(qualified_takes_table_name, 
                                                      schema=takes_table_schema,  
                                                      create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                      write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
    
    
logging.getLogger().setLevel(logging.ERROR)
