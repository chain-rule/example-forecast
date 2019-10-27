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
        options = dict(
            flags=[],
            runner='DataflowRunner',
            staging_location=_locate(config, 'staging'),
            temp_location=_locate(config, 'temporary'),
            setup_file=os.path.join('.', 'setup.py'),
            save_main_session=True,
        )
        options.update(config['pipeline'])
        super().__init__(options=beam.pipeline.PipelineOptions(**options))
        with tt_beam.Context(temp_dir=_locate(config, 'temporary')):
            _populate(self, config)


def _locate(config, *names):
    return os.path.join(config['output']['path'], *names)


def _populate(pipeline, config):
    schema = Schema(config['data']['schema'])

    def _analyze(example):
        return {
            name: _transform(name, example[name]) for name in example.keys()
        }

    def _filter(mode, example):
        return mode['name'] in example['mode']

    def _transform(name, value):
        if schema[name].transform is None:
            return value
        if schema[name].transform == 'z':
            return tft.scale_to_z_score(value)
        assert False

    query = open(config['data']['path']).read()
    source = beam.io.BigQuerySource(query=query, use_standard_sql=True)
    spec = schema.to_feature_spec()
    meta = dataset_metadata.DatasetMetadata(
        schema=dataset_schema.from_feature_spec(spec))
    data = pipeline \
        | 'read' >> beam.io.Read(source)
    transform_functions = {}
    for mode in config['modes']:
        if 'transform' in mode:
            continue
        name = mode['name']
        data_ = data \
            | '%s-filter' % name >> beam.Filter(partial(_filter, mode))
        transform_functions[name] = (data_, meta) \
            | '%s-analize' % name >> tt_beam.AnalyzeDataset(_analyze)
        path = _locate(config, name, 'transform')
        transform_functions[name] \
            | '%s-write-transform' % name >> transform_fn_io.WriteTransformFn(path)
    for mode in config['modes']:
        if not 'transform' in mode:
            continue
        name = mode['name']
        data_ = data \
            | '%s-filter' % name >> beam.Filter(partial(_filter, mode))
        if mode.get('shuffle', False):
            data_ = data_ \
                | '%s-shuffle' % name >> beam.transforms.Reshuffle()
        if mode['transform'] == 'identity':
            coder = tft.coders.ExampleProtoCoder(meta.schema)
        else:
            data_, meta_ = ((data_, meta), transform_functions[mode['transform']]) \
                | '%s-transform' % name >> tt_beam.TransformDataset()
            coder = tft.coders.ExampleProtoCoder(meta_.schema)
        path = _locate(config, name, 'records', 'part')
        data_ \
            | '%s-encode' % name >> beam.Map(coder.encode) \
            | '%s-write-records' % name >> beam.io.tfrecordio.WriteToTFRecord(path)
