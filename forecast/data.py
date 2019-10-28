import os
import tensorflow as tf
import tensorflow_transform as tft

from forecast.schema import Schema
from forecast.support import list_files


class Data:

    def __init__(self, config):
        self.path = list_files(config['path'])[-1]
        transform_path = list_files(config['transform_path'])[-1]
        self.transform = tft.TFTransformOutput(
            os.path.join(transform_path, 'analysis', 'transform'))
        self.schema = Schema(config['schema'])
        self.contextual_names = self.schema.select('contextual')
        self.sequential_names = self.schema.select('sequential')
        self.modes = config['modes']

    def create(self, name):

        def _preprocess_original(proto):
            spec = self.schema.to_feature_spec()
            example = tf.parse_single_example(proto, spec)
            for name in self.contextual_names:
                example[name] = tf.expand_dims(example[name], -1)
            for name in self.sequential_names:
                example[name] = tf.sparse.expand_dims(example[name], -1)
            example = self.transform.transform_raw_features(example)
            return (
                {
                    name: tf.reshape(example[name], [-1])
                    for name in self.contextual_names
                },
                {
                    name: tf.reshape( \
                        self.schema[name].to_dense(example[name]), [-1])
                    for name in self.sequential_names
                },
            )

        def _preprocess_transformed(proto):
            spec = self.transform.transformed_feature_spec()
            example = tf.io.parse_single_example(proto, spec)
            return (
                {
                    name: example[name]
                    for name in self.contextual_names
                },
                {
                    name: self.schema[name].to_dense(example[name])
                    for name in self.sequential_names
                },
            )

        def _postprocess(contextual, sequential):
            sequential = {
                name: self.schema[name].to_sparse(sequential[name])
                for name in self.sequential_names
            }
            return {**contextual, **sequential}

        def _shape():
            return (
                {
                    name: tf.TensorShape([])
                    for name in self.contextual_names
                },
                {
                    name: tf.TensorShape([None])
                    for name in self.sequential_names
                },
            )

        config = self.modes[name]
        pattern = [self.path, name, 'records', 'part-*']
        dataset = tf.data.Dataset.list_files(os.path.join(*pattern))
        if 'shuffle_macro' in config:
            dataset = dataset.shuffle(**config['shuffle_macro'])
        dataset = dataset \
            .interleave(tf.data.TFRecordDataset, **config['interleave'])
        if 'shuffle_micro' in config:
            dataset = dataset.shuffle(**config['shuffle_micro'])
        dataset = dataset \
            .map(locals()['_preprocess_' + config['spec']], **config['map']) \
            .padded_batch(padded_shapes=_shape(), **config['batch']) \
            .map(_postprocess, **config['map'])
        if 'prefetch' in config:
            dataset = dataset.prefetch(**config['prefetch'])
        if 'repeat' in config:
            dataset = dataset.repeat(**config['repeat'])
        return dataset

    def create_feature_columns(self, scope):

        def _process(name):
            return self.schema[name].to_feature_column(self.transform)

        return list(map(_process, getattr(self, scope + '_feature_names')))
