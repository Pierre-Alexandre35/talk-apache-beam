from __future__ import absolute_import
import argparse, logging, re
from past.builtins import unicode
import apache_beam as beam
from apache_beam.io import ReadFromText, WriteToText
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions


# generic parallel processing
class WordExtractingDoFn(beam.DoFn):
    def process(self, element):
        return re.findall(r'[\w\']+', element, re.UNICODE)


def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', dest='input')
    parser.add_argument('--output', dest='output')
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session


    # The pipeline will be run on exiting the with block.
    with beam.Pipeline(options=pipeline_options) as p:

        # Read the text file[pattern] into a PCollection.
        lines = p | 'Read' >> ReadFromText(known_args.input)


        # The ParDo only accepts DoFn
        # beam.Map is a one-to-one transform,
        # CombinePerKey works on two-element tuples. Groups the tuples by the first element (the key), and apply the provided function to the list 
        counts = (
            lines
            | 'Split' >>
            (beam.ParDo(WordExtractingDoFn()).with_output_types(unicode))
            | 'PairWIthOne' >> beam.Map(lambda x: (x, 1))
            | 'GroupAndSum' >> beam.CombinePerKey(sum))

        counts | 'Write' >> WriteToText(known_args.output)


if __name__ == '__main__':
    run()
