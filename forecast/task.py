class Task:

    def run(self, action, config):
        getattr(self, '_run_' + action)(config)

    def _run_preprocessing(self, config):
        pass
