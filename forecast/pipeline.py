import apache_beam as beam
import os
import tensorflow_transform as tft

from functools import partial
from tensorflow_transform.beam import impl as tt_beam
from tensorflow_transform.beam.tft_beam_io import transform_fn_io
from tensorflow_transform.tf_metadata import dataset_metadata
from tensorflow_transform.tf_metadata import dataset_schema

from forecast.schema import Schema


class Pipeline(beam.Pipeline):

    def __init__(self, config):
        # Set options pertaining to the Dataflow runner
        options = dict(
            runner='DataflowRunner',
            staging_location=_locate(config, 'staging'),
            temp_location=_locate(config, 'temporary'),
            setup_file=os.path.join('.', 'setup.py'),
            save_main_session=True,
            flags=[],
        )
        options.update(config['pipeline'])
        # Create and populate a Beam pipeline
        super().__init__(options=beam.pipeline.PipelineOptions(**options))
        with tt_beam.Context(temp_dir=_locate(config, 'temporary')):
            _populate(self, config)


def _locate(config, *names):
    return os.path.join(config['output']['path'], *names)


def _populate(pipeline, config):
    # Create a schema according to the configuration file
    schema = Schema(config['data']['schema'])

    def _analyze(example):
        return {
            name: _analyze_column(schema[name], example[name])
            for name in example.keys()
        }

    def _analyze_column(column, value):
        if column.transform is None:
            return value
        if column.transform == 'z':
            return tft.scale_to_z_score(value)
        # Define other transforms, such as vocabulary look-up
        assert False

    def _filter(mode, example):
        return mode['name'] in example['mode']

    # Read the SQL code
    query = open(config['data']['path']).read()
    # Create a BigQuery source
    source = beam.io.BigQuerySource(query=query, use_standard_sql=True)
    # Create metadata needed later
    spec = schema.to_feature_spec()
    meta = dataset_metadata.DatasetMetadata(
        schema=dataset_schema.from_feature_spec(spec))
    # Read data from BigQuery
    data = pipeline \
        | 'read' >> beam.io.Read(source)
    # Loop over modes whose purpose is analysis
    transform_functions = {}
    for mode in config['modes']:
        if 'transform' in mode:
            continue
        name = mode['name']
        # Select examples that belong to the current mode
        data_ = data \
            | name + '-filter' >> beam.Filter(partial(_filter, mode))
        # Analyze the examples
        transform_functions[name] = (data_, meta) \
            | name + '-analyze' >> tt_beam.AnalyzeDataset(_analyze)
        path = _locate(config, name, 'transform')
        # Store the transform function
        transform_functions[name] \
            | name + '-write-transform' >> transform_fn_io.WriteTransformFn(path)
    # Loop over modes whose purpose is transformation
    for mode in config['modes']:
        if not 'transform' in mode:
            continue
        name = mode['name']
        # Select examples that belong to the current mode
        data_ = data \
            | name + '-filter' >> beam.Filter(partial(_filter, mode))
        # Shuffle examples if needed
        if mode.get('shuffle', False):
            data_ = data_ \
                | name + '-shuffle' >> beam.transforms.Reshuffle()
        # Transform the examples using an appropriate transform function
        if mode['transform'] == 'identity':
            coder = tft.coders.ExampleProtoCoder(meta.schema)
        else:
            data_, meta_ = ((data_, meta), transform_functions[mode['transform']]) \
                | name + '-transform' >> tt_beam.TransformDataset()
            coder = tft.coders.ExampleProtoCoder(meta_.schema)
        path = _locate(config, name, 'examples', 'part')
        # Store the transformed examples as TFRecords
        data_ \
            | name + '-encode' >> beam.Map(coder.encode) \
            | name + '-write-examples' >> beam.io.tfrecordio.WriteToTFRecord(path)
