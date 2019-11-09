import os
import tensorflow as tf
import tensorflow_transform as tft

from forecast.schema import Schema
from forecast.support import list_files


class Data:

    def __init__(self, config):
        # Find the path with the latest preprocessed data by listing all entries
        # matching a given pattern and take the last one, assuming that the
        # alphabetical order is also chronological
        self.path = list_files(config['path'])[-1]
        # Find the path of the latest transforms using the same strategy
        transform_path = list_files(config['transform_path'])[-1]
        # Create a schema according to the configuration file
        self.schema = Schema(config['schema'])
        # Find the names of contextual columns
        self.contextual_names = self.schema.select('contextual')
        # Find the names of sequential columns
        self.sequential_names = self.schema.select('sequential')
        self.modes = config['modes']
        # Load all transforms
        transforms = [mode['transform'] for mode in self.modes.values()]
        self.transforms = {
            name: tft.TFTransformOutput( \
                os.path.join(transform_path, name, 'transform'))
            for name in set(transforms)
        }

    def create(self, name):
        config = self.modes[name]

        def _preprocess_transformed(proto):
            spec = self.transforms[config['transform']] \
                .transformed_feature_spec()
            example = tf.io.parse_single_example(proto, spec)
            return (
                {name: example[name] for name in self.contextual_names},
                {
                    # Convert the sequential columns from sparse to dense
                    name: self.schema[name].to_dense(example[name])
                    for name in self.sequential_names
                },
            )

        def _preprocess_untransformed(proto):
            spec = self.schema.to_feature_spec()
            example = tf.parse_single_example(proto, spec)
            for name in self.contextual_names:
                example[name] = tf.expand_dims(example[name], -1)
            for name in self.sequential_names:
                example[name] = tf.sparse.expand_dims(example[name], -1)
            example = self.transforms[config['transform']] \
                .transform_raw_features(example)
            return (
                {
                    name: tf.reshape(example[name], [-1])
                    for name in self.contextual_names
                },
                {
                    # Convert the sequential columns from sparse to dense
                    name: tf.reshape( \
                        self.schema[name].to_dense(example[name]), [-1])
                    for name in self.sequential_names
                },
            )

        def _postprocess(contextual, sequential):
            sequential = {
                # Convert the sequential columns from dense to sparse
                name: self.schema[name].to_sparse(sequential[name])
                for name in self.sequential_names
            }
            return {**contextual, **sequential}

        def _shape():
            return (
                {name: tf.TensorShape([]) for name in self.contextual_names},
                {
                    name: tf.TensorShape([None])
                    for name in self.sequential_names
                },
            )

        # List all files matching a given pattern
        pattern = [self.path, name, 'examples', 'part-*']
        dataset = tf.data.Dataset.list_files(os.path.join(*pattern))
        # Shuffle the files if needed
        if 'shuffle_macro' in config:
            dataset = dataset.shuffle(**config['shuffle_macro'])
        # Convert the files into datasets of examples stored as TFRecords and
        # amalgamate these datasets into one dataset of examples
        dataset = dataset \
            .interleave(tf.data.TFRecordDataset, **config['interleave'])
        # Shuffle the examples if needed
        if 'shuffle_micro' in config:
            dataset = dataset.shuffle(**config['shuffle_micro'])
        # Preprocess the examples with respect to a given spec, pad the examples
        # and form batches of different sizes, and postprocess the batches
        if config.get('transformed', False):
            _preprocess = _preprocess_transformed
        else:
            _preprocess = _preprocess_untransformed
        dataset = dataset \
            .map(_preprocess, **config['map']) \
            .padded_batch(padded_shapes=_shape(), **config['batch']) \
            .map(_postprocess, **config['map'])
        # Prefetch the batches if needed
        if 'prefetch' in config:
            dataset = dataset.prefetch(**config['prefetch'])
        # Repeat the data once the source is exhausted if needed
        if 'repeat' in config:
            dataset = dataset.repeat(**config['repeat'])
        return dataset

    def create_feature_columns(self, scope):

        def _process(name):
            return self.schema[name].to_feature_column(self.transform)

        return list(map(_process, getattr(self, scope + '_feature_names')))
