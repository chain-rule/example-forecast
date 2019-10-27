class Task:

    def run(self, action, config):
        getattr(self, '_run_' + action)(config)

    def _run_preprocessing(self, config):
        from forcast.pipeline import Pipeline
        Pipeline(config).run().wait_until_finish()
