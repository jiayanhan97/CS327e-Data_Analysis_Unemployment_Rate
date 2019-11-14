import logging,os,datetime
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

# DoFn to perform on each element in the input PCollection.
class StateName(beam.DoFn):
  def process(self, element):
    record = element
    state = record.get('State')
    year = record.get('Year')
    rate = record.get('Rate')
    global state_abb
    if state == 'Alabama':
      state_abb = 'AL'
    if state == 'Alaska':
      state_abb = 'AK'
    if state == 'Arizona':
      state_abb = 'AZ'
    if state == 'California':
      state_abb = 'CA'
    if state == 'Colorado':
      state_abb = 'CO'
    if state == 'Connecticut':
      state_abb = 'CT'
    if state == 'Delaware':
      state_abb = 'DE'
    if state == 'Florida':
      state_abb = 'FL'
    if state == 'Georgia':
      state_abb = 'GA'
    if state == 'Hawaii':
      state_abb = 'HI'
    if state == 'Idaho':
      state_abb = 'ID'
    if state == 'Illinois':
      state_abb = 'IL'
    if state == 'Indiana':
      state_abb = 'IN'
    if state == 'Iowa':
      state_abb = 'IA'
    if state == 'Kansas':
      state_abb = 'KS'
    if state == 'Kentucky':
      state_abb = 'KY'
    if state == 'Louisiana':
      state_abb = 'LA'
    if state == 'Maine':
      state_abb = 'ME'
    if state == 'Maryland':
      state_abb = 'MD'
    if state == 'Massachusetts':
      state_abb = 'MA'
    if state == 'Michigan':
      state_abb = 'MI'
    if state == 'Minnesota':
      state_abb = 'MN'
    if state == 'Mississippi':
      state_abb = 'MS'
    if state == 'Missouri':
      state_abb = 'MO'
    if state == 'Montana':
      state_abb = 'MT'
    if state == 'Nebraska':
      state_abb = 'NE'
    if state == 'Nevada':
      state_abb = 'NV'
    if state == 'Michigan':
      state_abb = 'MI'
    if state == 'New Hampshire':
      state_abb = 'NH'
    if state == 'New Jersey':
      state_abb = 'NJ'
    if state == 'New Mexico':
      state_abb = 'NM'
    if state == 'New York':
      state_abb = 'NY'
    if state == 'North Carolina':
      state_abb = 'NC'
    if state == 'North Dakota':
      state_abb = 'ND'
    if state == 'Ohio':
      state_abb = 'OH'
    if state == 'Oklahoma':
      state_abb = 'OK'
    if state == 'Oregon':
      state_abb = 'OR'
    if state == 'Pennsylvania':
      state_abb = 'PA'
    if state == 'Rhode Island':
      state_abb = 'RI'
    if state == 'South Carolina':
      state_abb = 'SC'
    if state == 'South Dakota':
      state_abb = 'SD'
    if state == 'Tennessee':
      state_abb = 'TN'
    if state == 'Texas':
      state_abb = 'TX'
    if state == 'Utah':
      state_abb = 'UT'
    if state == 'Vermont':
      state_abb = 'VT'
    if state == 'Virginia':
      state_abb = 'VA'
    if state == 'Washington':
      state_abb = 'WA'
    if state == 'West Virginia':
      state_abb = 'WV'
    if state == 'Wisconsin':
      state_abb = 'WI'
    if state == 'Wyoming':
      state_abb = 'WY'     

    result = {"Year":year,"State":state_abb,"Rate": rate}
    return [result]

PROJECT_ID = os.environ['PROJECT_ID']
BUCKET = os.environ['BUCKET']
DIR_PATH = BUCKET + '/output/' + datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S') + '/'

# Project ID is needed for BigQuery data source, even for local execution.
options = {
    'runner': 'DataflowRunner',
    'job_name': 'rate',
    'project': PROJECT_ID,
    'temp_location': BUCKET + '/temp',
    'staging_location': BUCKET + '/staging',
    'machine_type': 'n1-standard-8',
    'num_workers': 8
}
opts = beam.pipeline.PipelineOptions(flags=[], **options)

with beam.Pipeline('DataflowRunner', options=opts) as p:
    
    query_results = p | beam.io.Read(beam.io.BigQuerySource(query='SELECT Year,State,Rate FROM Unemployment.unemployment_rate'))

    # write PCollection to a log file
    query_results | 'Write to File 1' >> WriteToText(DIR_PATH+'unemployment_query.txt')

    #apply Pardo on the Pcollection
    state_pcoll = query_results | 'Create State abb' >> beam.ParDo(StateName())
    
    # write PCollection to a file
    state_pcoll | 'Write to File 2' >> WriteToText(DIR_PATH+'output_unemployment.txt')
    
    qualified_takes_table_name = 'han97jiayan:Unemployment.unemployment_transform_cluster'
    takes_table_schema = 'Year:INTEGER,State:STRING,Rate:FLOAT'
    
    state_pcoll | 'Write Takes to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(qualified_takes_table_name, 
                                                      schema=takes_table_schema,  
                                                      create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                      write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
    
    
logging.getLogger().setLevel(logging.ERROR)
