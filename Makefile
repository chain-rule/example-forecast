all:

training-preprocessing:
	python -m forecast.main \
		--action preprocessing \
		--config configs/training/preprocessing.json

.PHONY: all training-preprocessing
