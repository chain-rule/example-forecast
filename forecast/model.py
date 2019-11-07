import tensorflow as tf


class Model:

    def __init__(self, data, config):
        self.data = data
        self.config = config

    def create(self):
        return _create(self.data, self.config)


def _create(data, config):
    contextual_inputs, contextual_outputs = \
        _create_contextual_inputs(data, config)
    sequential_inputs, sequential_outputs, _ = \
        _create_sequential_inputs(data, config)
    # Construct the model
    # ...
    # model = tf.keras.Model(
    #     inputs={**contextual_inputs, **sequential_inputs},
    #     outputs=outputs,
    # )
    # model.compile(...)
    # return model


def _create_contextual_inputs(data, config):
    inputs = {
        name: tf.keras.Input(name=name, shape=(), dtype=data.schema[name].kind)
        for name in data.contextual_feature_names
    }
    layer = tf.keras.layers.DenseFeatures(
        feature_columns=data.create_feature_columns('contextual'))
    return inputs, layer(inputs)


def _create_sequential_inputs(data, config):
    inputs = {
        name: tf.keras.Input(name=name,
                             shape=(None,),
                             dtype=data.schema[name].kind,
                             sparse=True)
        for name in data.sequential_feature_names
    }
    layer = tf.keras.experimental.SequenceFeatures(
        feature_columns=data.create_feature_columns('sequential'))
    return (inputs, *layer(inputs))
