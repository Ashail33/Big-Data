import apache_beam as beam
import apache_beam.transforms.window as window
from apache_beam.io import ReadFromPubSub
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.transforms.trigger import AfterProcessingTime, AccumulationMode

import argparse
import datetime
import logging

logging.basicConfig(
    format="%(asctime)s : %(levelname)s : %(name)s : %(message)s", level=logging.INFO
)
logging = logging.getLogger(__name__)

def timestamp2str(t, fmt='%Y-%m-%d %H:%M:%S.000'):
  """Converts a unix timestamp into a formatted string."""
  return datetime.datetime.strptime(t, fmt)

class TeamScoresDict(beam.DoFn):
  """Formats the data into a dictionary of BigQuery columns with their values
  Receives a (team, score) pair, extracts the window start timestamp, and
  formats everything together into a dictionary. The dictionary is in the format
  {'bigquery_column': value}
  """

  
  def process(self, team_score, window=beam.DoFn.WindowParam):
    user_id_url,bytes = team_score
    user_id,url=user_id_url
    start = datetime.datetime.fromtimestamp(window.start)
    yield [user_id,url,bytes,start,datetime.datetime.now()]
    
class ExtractAndSumScore(beam.PTransform):
  """A transform to extract key/score information and sum the scores.
  The constructor argument `field` determines whether 'team' or 'user' info is
  extracted.
  """
  def __init__(self, field):

    beam.PTransform.__init__(self)
    self.field = field

  def expand(self, pcoll):
    return (
        pcoll
        | beam.Map(lambda elem: ((elem['user_id'],elem['url']), elem['bytes']))
        | beam.CombinePerKey(sum))

class JobOptions(PipelineOptions):
    """
    these are commmand line options for cli usage
    """
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument("--input", required=True, help="pubsub input topic")

def custom_timestamp(element):
    """
    you need to tell Beam which field is your event time. This stub is assuming the 'message' is
    a row with a field 'timestamp', which is being indexed here. Do here as you wish.
    :param message:
    :return:
    """
    #def process(self, element):
    ts = datetime.datetime.strptime(element['timestamp'], "%Y-%m-%d %H:%M:%S")
                                    #ts = message
    return beam.window.TimestampedValue(element,ts.timestamp()) 

class Split(beam.DoFn):

    def process(self, element):
        user_id,fullname,url,timestamp,bytes = element.decode("utf-8").split(";")

#         return [{'User':User,'Gender':Gender,'Age':Age,'Address':Address.replace("-", ","),'Date_joined':Date_joined.replace("/", "-")}] 
        return [{'user_id':user_id,'fullname':fullname,'url':url,'timestamp':timestamp,'bytes':int(bytes)}] 


def run(argv=None, save_main_session=True):
    """
    run function to process cli args and run your program
    :param argv:
    :param save_main_session:
    :return:
    """

    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    job_options = pipeline_options.view_as(JobOptions)


    logging.info("-----------------------------------------------------------")
    logging.info("               Streaming with Pub/Sub emulator             ")
    logging.info("-----------------------------------------------------------")


    def print_row(element):
        print(element)

    source = ReadFromPubSub(subscription=str(job_options.input))

    ###
    #  STREAMING BEAM: add the necessary pipeline stages along with whatever functions you require in this file
    ###
    p = beam.Pipeline(options=pipeline_options)
    lines = (
        p
        | "read" >> source
        | 'transform' >> beam.ParDo(Split())
        | 'AddEventTimestamps' >> beam.Map(lambda elem:custom_timestamp(elem))
        | 'FixedWindowsTeam' >> beam.WindowInto(window.FixedWindows(60),trigger=beam.transforms.trigger.AfterWatermark(
        late=beam.transforms.trigger.AfterCount(1)),
    accumulation_mode=AccumulationMode.ACCUMULATING)
 #       | 'FixedWindowsTeam' >> beam.WindowInto(window.FixedWindows(60))
        | 'ExtractAndSumScore' >> ExtractAndSumScore('field')
        | 'TeamScoresDict' >> beam.ParDo(TeamScoresDict())
        | beam.Map(print_row)
    )
    result = p.run()
    result.wait_until_finish()

if __name__ == "__main__":
    run()
